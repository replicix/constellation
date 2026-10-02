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
use crate::uring::mem::HEADER_SZ;
use crate::uring::mem::OP_IN_OFFSET;
use crate::uring::mem::PAYLOAD_SZ_OFFSET;
use crate::uring::ring::WAKE;

const IORING_OP_POLL_ADD: u8 = 6;
const IORING_OP_URING_CMD: u8 = 46;
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
    queued: HashMap<u16, VecDeque<(Vec<u8>, usize)>>,
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
            .push_back((request.to_vec(), op_in_len));
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
            IORING_OP_URING_CMD if self.hung_up => {
                self.complete(ring, user_data, -libc::ENOTCONN, 0)
            }
            IORING_OP_URING_CMD => {
                let qid = u16_at(sqe, 64);
                let commit_id = u64_at(sqe, 56);
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
                            },
                        );
                        self.deliver();
                    }
                    c if c == abi::fuse_uring_cmd::FUSE_IO_URING_CMD_COMMIT_AND_FETCH as u32 => {
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
                        // SAFETY: the entry was with userspace until this commit, whose
                        // submit hands its buffers back; the header area holds the 16-byte
                        // out header and the trailer, the payload `payload_sz` bytes, as the
                        // kernel's `fuse_uring_copy_from_ring` reads them.
                        let reply = unsafe {
                            let payload_sz =
                                ptr::read_unaligned(b.header.add(PAYLOAD_SZ_OFFSET).cast::<u32>())
                                    as usize;
                            let mut reply = vec![0u8; OUT_HEADER_SZ + payload_sz.min(b.payload_len)];
                            ptr::copy_nonoverlapping(b.header, reply.as_mut_ptr(), OUT_HEADER_SZ);
                            ptr::copy_nonoverlapping(
                                b.payload,
                                reply.as_mut_ptr().add(OUT_HEADER_SZ),
                                reply.len() - OUT_HEADER_SZ,
                            );
                            reply
                        };
                        self.replies.push_back(reply);
                        replied.notify_all();
                        self.deliver();
                    }
                    _ => self.complete(ring, user_data, -libc::EINVAL, 0),
                }
            }
            _ => self.complete(ring, user_data, -libc::EINVAL, 0),
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
            let Some((request, op_in_len)) = self.queued.get_mut(&qid).and_then(VecDeque::pop_front)
            else {
                continue;
            };
            let entry = self.entries.get_mut(&ud).unwrap();
            let b = entry.buffers;
            let unique = u64_at(&request, 8);
            let op_in = &request[IN_HEADER_SZ..IN_HEADER_SZ + op_in_len];
            let payload = &request[IN_HEADER_SZ + op_in_len..];
            assert!(
                payload.len() <= b.payload_len,
                "a request's payload fits the entry's buffer"
            );
            // SAFETY: the entry is parked with this "kernel" (its REGISTER or commit is
            // pending), so nothing in userspace touches its buffers; every write lies inside
            // the header area or the payload buffer the REGISTER named.
            unsafe {
                ptr::copy_nonoverlapping(request.as_ptr(), b.header, IN_HEADER_SZ);
                ptr::copy_nonoverlapping(op_in.as_ptr(), b.header.add(OP_IN_OFFSET), op_in.len());
                ptr::write_unaligned(b.header.add(COMMIT_ID_OFFSET).cast::<u64>(), unique);
                ptr::write_unaligned(
                    b.header.add(PAYLOAD_SZ_OFFSET).cast::<u32>(),
                    payload.len() as u32,
                );
                ptr::copy_nonoverlapping(payload.as_ptr(), b.payload, payload.len());
            }
            entry.busy = Some(unique);
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
    pub(crate) fn submit_and_wait(&mut self, timed: bool) -> io::Result<usize> {
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
            let timeout = if timed {
                PollTimeout::from(10u16)
            } else {
                PollTimeout::NONE
            };
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
                state.rings[self.index]
                    .cq
                    .push_back((WAKE, libc::POLLIN as i32, IORING_CQE_F_MORE));
                return Ok(n);
            }
        }
    }

    pub(crate) fn reap(&mut self, out: &mut impl Extend<(u64, i32, u32)>) {
        let mut state = self.kernel.shared.state.lock();
        out.extend(state.rings[self.index].cq.drain(..));
    }
}
