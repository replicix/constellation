//! CONSTELLATION PATCH (io-uring): this whole file is Constellation's, not upstream
//! fuser's (vendor/fuser/CONSTELLATION-PATCH.md, patches/0002-io-uring-transport.patch).
//!
//! One ring: its entries, their state machine, the commit protocol and the ring thread's loop.
//!
//! Every SQE of a ring is pushed and submitted by the ring's own thread. Other threads reply by
//! writing the entry's buffers under the entry state machine, queueing the entry in
//! `Live::pending` and waking the ring thread through an eventfd. Lock order is an entry's
//! `state`, then `live`; neither is held while buffers are written or the io_uring is used.

use std::fmt;
use std::fs::File;
use std::io;
use std::io::IoSlice;
use std::mem::ManuallyDrop;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::ptr;
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::thread::ThreadId;

use io_uring::IoUring;
use io_uring::cqueue;
use io_uring::opcode;
use io_uring::squeue;
use io_uring::types;
use log::debug;
use log::error;
use log::warn;
use nix::sys::eventfd::EfdFlags;
use nix::sys::eventfd::EventFd;
use parking_lot::Mutex;
use smallvec::SmallVec;
use zerocopy::IntoBytes;

use crate::dev_fuse::DevFuse;
use crate::ll::Errno;
use crate::ll::fuse_abi as abi;
use crate::reply::pread_full;
use crate::uring::mem::FLAGS_OFFSET;
use crate::uring::mem::HEADER_SZ;
use crate::uring::mem::PAYLOAD_SZ_OFFSET;
use crate::uring::mem::POOL_OFFSET_OFFSET;
use crate::uring::mem::PoolMemory;
use crate::uring::mem::RingMemory;
use crate::uring::staging::StagingError;
use crate::uring::staging::stage_request;

/// `user_data` of the eventfd poll. Entry `user_data` is `qid << 32 | idx`, so it never gets here.
pub(crate) const WAKE: u64 = u64::MAX;
/// `io_uring_sqe.len` is a `u32` at this byte offset in the uapi layout.
const SQE_LEN_OFFSET: usize = 24;
const OUT_HEADER_SZ: usize = size_of::<abi::fuse_out_header>();
/// Largest SQ io_uring accepts; the CQ may be twice that.
pub(crate) const IORING_MAX_ENTRIES: usize = 32768;
/// CONSTELLATION PATCH (io-uring): `user_data` bit of an entry's `IORING_OP_READ_FIXED`
/// (`RingCommit::read_fixed`). Entry `user_data` stays below bit 48, and `WAKE`, which has
/// every bit set, is told apart before this bit is looked at.
const READ_TAG: u64 = 1 << 63;
/// CONSTELLATION PATCH (io-uring): `user_data` of the setup commands (`ADD_QUEUE`,
/// `ADD_BUFPOOL`), which complete before anything else is submitted.
const SETUP_TAG: u64 = 1 << 62;
/// CONSTELLATION PATCH (io-uring): the buffer-table index of a registered buffer pool. The
/// entries' zero-copy slots follow it: entry `i` owns slot `i + 1`, as libfuse's draft does.
const POOL_BUF_INDEX: u16 = 0;
/// CONSTELLATION PATCH (io-uring): io_uring's largest buffer table, `IORING_MAX_REG_BUFFERS`.
const IORING_MAX_REG_BUFFERS: usize = 1 << 14;

/// Called once per fetched request with the commit handle and the contiguous request bytes.
/// The slice is valid only for the duration of the call.
pub(crate) trait FetchHandler: Send {
    fn handle(&mut self, commit: RingCommit, request: &[u8]);
}

impl<F: FnMut(RingCommit, &[u8]) + Send> FetchHandler for F {
    fn handle(&mut self, commit: RingCommit, request: &[u8]) {
        self(commit, request)
    }
}

/// Submission and completion queue sizes for a ring of `entries` entries. The kernel needs
/// `cq >= sq` when the CQ size is given explicitly.
pub(crate) fn ring_sizes(entries: usize) -> (u32, u32) {
    let sq = entries.next_power_of_two().clamp(8, IORING_MAX_ENTRIES);
    let cq = (2 * entries)
        .next_power_of_two()
        .clamp(sq, 2 * IORING_MAX_ENTRIES);
    if entries > IORING_MAX_ENTRIES {
        warn!(
            "io_uring: {entries} entries per ring exceed the completion queue; overflow handling \
             will be used"
        );
    }
    (sq as u32, cq as u32)
}

/// The io_uring instance, owned by the ring thread alone; nothing else may touch it.
///
/// CONSTELLATION PATCH (io-uring): the instance is one of two backends. `Kernel` is the real
/// io_uring; `Memory` is `InMemoryRingKernel`'s (`uring::memory`), which decodes the same SQEs
/// the way the kernel does and answers with the same CQEs, so that everything above this type
/// -- the ring thread's loop, the entry state machine, the commit protocol, the session -- runs
/// unchanged in a test with no io_uring, no mount and no root (plan 38 §6).
pub(crate) struct RingIo {
    io: Backend,
    /// CQEs reaped right after the REGISTER submit (to see the kernel's synchronous refusals);
    /// whatever else was among them is handed to the first pass of `serve`.
    early: SmallVec<[(u64, i32, u32); 8]>,
    #[cfg(test)]
    hooks: test::IoHooks,
}

/// CONSTELLATION PATCH (io-uring): see `RingIo`.
/// The kernel's instance is boxed: it is several times the size of the in-memory one, and
/// lives as long as the ring, so the one allocation buys nothing back by being inline.
enum Backend {
    Kernel(Box<IoUring<squeue::Entry128, cqueue::Entry>>),
    Memory(crate::uring::memory::MemoryRing),
}

impl RingIo {
    /// `io_uring_setup`; `EPERM` and `ENOSYS` here mean the environment forbids io_uring,
    /// `EINVAL` a kernel before 6.1 that lacks the setup flags.
    pub(crate) fn open(sq_entries: u32, cq_entries: u32) -> io::Result<Self> {
        let io = IoUring::<squeue::Entry128, cqueue::Entry>::builder()
            .dontfork()
            .setup_submit_all()
            .setup_cqsize(cq_entries)
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_r_disabled()
            .build(sq_entries)?;
        Ok(Self {
            io: Backend::Kernel(Box::new(io)),
            early: SmallVec::new(),
            #[cfg(test)]
            hooks: test::IoHooks::default(),
        })
    }

    /// CONSTELLATION PATCH (io-uring): a ring served by an `InMemoryRingKernel`.
    pub(crate) fn memory(ring: crate::uring::memory::MemoryRing) -> Self {
        Self {
            io: Backend::Memory(ring),
            early: SmallVec::new(),
            #[cfg(test)]
            hooks: test::IoHooks::default(),
        }
    }

    /// The real io_uring, for tests that poke at it directly.
    #[cfg(test)]
    pub(crate) fn uring(&mut self) -> &mut IoUring<squeue::Entry128, cqueue::Entry> {
        match &mut self.io {
            Backend::Kernel(io) => io,
            Backend::Memory(_) => panic!("not a kernel io_uring"),
        }
    }

    /// Enables the ring and binds the calling thread as its issuer; every later
    /// `io_uring_enter` must come from that thread.
    fn enable(&mut self) -> io::Result<()> {
        #[cfg(test)]
        self.hooks.before_enable()?;
        match &mut self.io {
            Backend::Kernel(io) => io.submitter().register_enable_rings(),
            Backend::Memory(_) => Ok(()),
        }
    }

    fn submit(&mut self) -> io::Result<usize> {
        #[cfg(test)]
        self.hooks.before_submit()?;
        match &mut self.io {
            Backend::Kernel(io) => io.submit(),
            Backend::Memory(m) => m.submit(),
        }
    }

    /// Submits the queue and waits for one CQE, at most `timeout`: `ETIME` (or, from the
    /// in-memory backend, no CQE) when it passed with none.
    ///
    /// CONSTELLATION PATCH (io-uring): every wait is bounded now (`IDLE_WAIT`), not only the
    /// one after the eventfd poll died.
    fn submit_and_wait(&mut self, timeout: Duration) -> io::Result<usize> {
        if !self.early.is_empty() {
            return self.submit();
        }
        match &mut self.io {
            Backend::Kernel(io) => {
                let ts = types::Timespec::from(timeout);
                let args = types::SubmitArgs::new().timespec(&ts);
                io.submitter().submit_with_args(1, &args)
            }
            Backend::Memory(m) => m.submit_and_wait(timeout),
        }
    }

    /// Pushes one SQE, making room with a `submit` when the queue is full. `Err` means the
    /// SQE was not pushed.
    fn push_or_submit(&mut self, sqe: &squeue::Entry128) -> io::Result<()> {
        let io = match &mut self.io {
            Backend::Kernel(io) => io,
            Backend::Memory(m) => {
                m.push(sqe_bytes(sqe.clone()));
                return Ok(());
            }
        };
        // SAFETY: every buffer an SQE names lives in `Ring::mem`, which stays mapped while a
        // command is pending (`Drop for Ring`), or in a `RingEntry::iov` that lives as long.
        if unsafe { io.submission().push(sqe) }.is_ok() {
            return Ok(());
        }
        {
            #[cfg(test)]
            self.hooks.before_submit()?;
            io.submit()
        }?;
        // SAFETY: as above.
        unsafe { io.submission().push(sqe) }
            .map_err(|_| io::Error::other("submission queue still full after submit"))
    }

    /// CONSTELLATION PATCH (io-uring): registers this ring's buffer table: `slots` sparse
    /// slots for the kernel to register zero-copied requests' pages into, and `pool` -- the
    /// ring's buffer pools as one fixed buffer, which pins every page of it -- at
    /// `POOL_BUF_INDEX`. Only before the first REGISTER, from the ring's own thread.
    fn register_buffer_table(&mut self, pool: Option<libc::iovec>, slots: u32) -> io::Result<()> {
        match &mut self.io {
            Backend::Kernel(io) => {
                io.submitter().register_buffers_sparse(slots)?;
                if let Some(iov) = pool {
                    // SAFETY: the pool is mapped until the ring's buffers are unregistered or
                    // its io_uring is closed (`Drop for Ring` leaks it while commands pend).
                    let updated =
                        unsafe { io.submitter().register_buffers_update(0, &[iov], None) };
                    if let Err(err) = updated {
                        let _ = io.submitter().unregister_buffers();
                        return Err(err);
                    }
                }
                Ok(())
            }
            Backend::Memory(m) => m.register_buffer_table(pool, slots),
        }
    }

    /// CONSTELLATION PATCH (io-uring): undoes `register_buffer_table`, releasing the pins.
    fn unregister_buffer_table(&mut self) -> io::Result<()> {
        match &mut self.io {
            Backend::Kernel(io) => io.submitter().unregister_buffers(),
            Backend::Memory(m) => m.unregister_buffer_table(),
        }
    }

    /// CONSTELLATION PATCH (io-uring): issues one setup command and returns its result. The
    /// kernel completes `ADD_QUEUE` and `ADD_BUFPOOL` inline (they never return
    /// `-EIOCBQUEUED`), so the wait for the one CQE cannot hang; nothing else is in flight on
    /// the ring yet.
    fn setup_cmd(&mut self, sqe: squeue::Entry128) -> io::Result<()> {
        let sqe = sqe.user_data(SETUP_TAG);
        self.push_or_submit(&sqe)?;
        #[cfg(test)]
        self.hooks.before_submit()?;
        let res = match &mut self.io {
            Backend::Kernel(io) => {
                io.submit_and_wait(1)?;
                io.completion()
                    .find(|c| c.user_data() == SETUP_TAG)
                    .map(|c| c.result())
            }
            Backend::Memory(m) => {
                m.submit()?;
                let mut cqes: SmallVec<[(u64, i32, u32); 4]> = SmallVec::new();
                m.reap(&mut cqes);
                cqes.iter().find(|c| c.0 == SETUP_TAG).map(|c| c.1)
            }
        };
        match res {
            Some(res) if res < 0 => Err(io::Error::from_raw_os_error(-res)),
            Some(_) => Ok(()),
            None => Err(io::Error::other("no completion for a setup command")),
        }
    }

    /// Every CQE available now, as `(user_data, res, flags)`, the early ones first.
    fn reap(&mut self) -> SmallVec<[(u64, i32, u32); 64]> {
        let mut cqes: SmallVec<[(u64, i32, u32); 64]> = self.early.drain(..).collect();
        match &mut self.io {
            Backend::Kernel(io) => {
                cqes.extend(
                    io.completion()
                        .map(|c| (c.user_data(), c.result(), c.flags())),
                );
            }
            Backend::Memory(m) => m.reap(&mut cqes),
        }
        cqes
    }
}

/// CONSTELLATION PATCH (io-uring): why `Ring::locate_payload` refused a fetch, each kind
/// logged once.
#[derive(Debug, Clone, Copy)]
enum Refusal {
    Offset = 0,
    Oversize = 1,
    ZeroCopied = 2,
}

/// CONSTELLATION PATCH (io-uring): whether the kernel reads a payload from the reply to
/// `opcode`, i.e. whether its `fuse_args.out_numargs` is non-zero. Those without (and the
/// ones never answered) get no pool buffer unless they carry input to copy, so on a pool queue
/// their reply may carry no payload (`Ring::locate_payload`). Unknown opcodes count as having
/// out args: the kernel would not send them, and a wrong `true` only costs the guard.
fn has_out_args(opcode: u32) -> bool {
    use abi::fuse_opcode::*;
    !matches!(
        abi::fuse_opcode::try_from(opcode),
        Ok(FUSE_FORGET
            | FUSE_UNLINK
            | FUSE_RMDIR
            | FUSE_RENAME
            | FUSE_RENAME2
            | FUSE_RELEASE
            | FUSE_RELEASEDIR
            | FUSE_FSYNC
            | FUSE_FSYNCDIR
            | FUSE_SETXATTR
            | FUSE_REMOVEXATTR
            | FUSE_FLUSH
            | FUSE_SETLK
            | FUSE_SETLKW
            | FUSE_ACCESS
            | FUSE_INTERRUPT
            | FUSE_DESTROY
            | FUSE_BATCH_FORGET
            | FUSE_FALLOCATE)
    )
}

/// An SQE as the 128 bytes the kernel reads.
pub(crate) fn sqe_bytes(sqe: squeue::Entry128) -> [u8; 128] {
    // SAFETY: as in `set_sqe_len`.
    unsafe { std::mem::transmute(sqe) }
}

/// State shared between the ring thread and every `RingCommit`.
pub(crate) struct Ring {
    index: usize,
    /// CONSTELLATION PATCH (io-uring): entries per queue.
    depth: u32,
    /// Whether an unmount will end the connection when the session is dropped without
    /// running; `Session::from_fd` sessions have nothing that will.
    mounted: bool,
    /// The `/dev/fuse` descriptor named in every SQE.
    device: Arc<DevFuse>,
    /// Set at the top of `thread_main`; commits from that thread skip the eventfd because the
    /// thread drains `pending` itself before waiting.
    ring_thread: OnceLock<ThreadId>,
    wake: EventFd,
    live: Mutex<Live>,
    entries: Box<[RingEntry]>,
    /// Dropped only when `live.in_kernel` is zero; otherwise leaked, see `Drop`.
    mem: ManuallyDrop<RingMemory>,
    /// CONSTELLATION PATCH (io-uring): fault injection -- every REGISTER names one iovec
    /// instead of two, which the kernel refuses with `EINVAL` before it records anything
    /// (`fuse_uring_get_iovec_from_sqe`). Set only through `Config::io_uring_malformed_register`.
    malformed_register: std::sync::atomic::AtomicBool,
    /// CONSTELLATION PATCH (io-uring): where this ring counts what `note_stranded` finds. Set
    /// only through `Config::io_uring_health`, before any request is served.
    health: OnceLock<super::RingHealth>,
    /// CONSTELLATION PATCH (io-uring): when `note_stranded` last logged (`mono_ms`, 0 never).
    stranded_logged: std::sync::atomic::AtomicU64,
    /// CONSTELLATION PATCH (io-uring): told about every lock-wait budget downgrade
    /// (`HeldRequest::downgrade_lock_wait`). Set only through
    /// `Config::io_uring_lock_wait_downgrades`, before any request is served.
    lock_wait_downgrades: OnceLock<super::LockWaitDowngrades>,
    /// CONSTELLATION PATCH (io-uring): the pool memory reserved for this ring's queues and
    /// whether to register it as a fixed buffer (`Ring::reserve_pools`), until `set_up_queues`
    /// either hands it to the kernel or drops it.
    pool_plan: Mutex<Option<(PoolMemory, bool)>>,
    /// CONSTELLATION PATCH (io-uring): set by `set_up_queues`, before the first REGISTER, when
    /// this ring's queues take their payload buffers from pools; unset, every entry has its
    /// own payload buffer, as on 7.42.
    pools: OnceLock<Pools>,
    /// CONSTELLATION PATCH (io-uring): the barriers `RingSet::start` holds between the buffer
    /// tables of every ring and the first `ADD_QUEUE` of any, and between the pool setup of
    /// every ring and the first REGISTER of any: where this ring reports each round, and the
    /// answers it waits for. `None` for a ring outside a `RingSet` (the unit tests), which
    /// registers at once.
    setup_gate: Mutex<Option<SetupGate>>,
    /// CONSTELLATION PATCH (io-uring): whether the whole session is zero-copy, as
    /// `RingSet::start` settled it before any ring registered (`RingCommit::transport`).
    /// Unset outside a `RingSet`.
    session_zero_copy: OnceLock<bool>,
    /// CONSTELLATION PATCH (io-uring): the memfd a reply to a zero-copied request is bounced
    /// through when it is given as bytes rather than with `read_fixed` (`Ring::bounce_in`).
    /// Created on first use.
    bounce: OnceLock<Result<File, String>>,
    #[cfg(test)]
    hooks: test::RingHooks,
}

/// CONSTELLATION PATCH (io-uring): see `Ring::setup_gate`. Two rounds, each a report to
/// `RingSet::start` and its answer: the buffer table (`table`, then `queues`: whether every
/// ring's table is in place, so this one goes on to its zero-copy queues), then the queues and
/// pools (`setup`, then `proceed`: register).
pub(crate) struct SetupGate {
    pub(crate) table: mpsc::Sender<io::Result<TableSetup>>,
    pub(crate) queues: mpsc::Receiver<bool>,
    pub(crate) setup: mpsc::Sender<io::Result<PoolSetup>>,
    pub(crate) proceed: mpsc::Receiver<()>,
}

/// CONSTELLATION PATCH (io-uring): how the first round of one ring's pool setup ended, its
/// buffer table (`Ring::set_up_table`).
#[derive(Debug)]
pub(crate) enum TableSetup {
    /// The session did not ask for zero-copy.
    NotAsked,
    /// The table was refused, which leaves nothing in the kernel; this is why.
    Refused(String),
    /// The table is registered; the ring waits to hear whether every other ring's is too.
    Ready,
}

/// CONSTELLATION PATCH (io-uring): how the pool setup of one ring ended (`Ring::set_up_queues`).
#[derive(Debug)]
pub(crate) enum PoolSetup {
    /// The session did not ask for zero-copy: every entry has its own payload buffer.
    NotAsked,
    /// Zero-copy was asked for and refused before anything was created in the kernel; every
    /// entry has its own payload buffer, and this is why.
    Degraded(String),
    /// The ring's queues take their payload buffers from pools; `zero_copy` when every one of
    /// them is a zero-copy queue. `note` names a partial refusal (a queue the kernel would
    /// only create without zero-copy, a pool it would only take unregistered).
    Pools {
        zero_copy: bool,
        note: Option<String>,
    },
}

/// CONSTELLATION PATCH (io-uring): the pools of a ring whose queues have them.
struct Pools {
    /// Dropped only with the ring and only when `live.in_kernel` is zero; see `Drop for Ring`.
    mem: ManuallyDrop<PoolMemory>,
    /// Per queue of the ring, by `RingEntry::queue`.
    queues: Vec<QueuePool>,
}

/// CONSTELLATION PATCH (io-uring): one queue's pool setup, as the kernel accepted it.
#[derive(Debug, Clone, Copy)]
struct QueuePool {
    /// Created with `FUSE_URING_ZERO_COPY`: its entries carry their buffer-table slot.
    zero_copy: bool,
    /// The pool is reached through the fixed buffer at `POOL_BUF_INDEX`, so every REGISTER and
    /// COMMIT_AND_FETCH of the queue must name it (`fuse_uring_cmd_index_ok`).
    fixed: bool,
}

/// Exit decision, in-kernel accounting and the commit queue, under one lock.
struct Live {
    /// Entries whose command is pending in the kernel or queued in `pending` to become so.
    in_kernel: usize,
    /// Entries fetched and not yet handed back: `Dispatching`, `Deferred`, `Dispatched` or
    /// `Committing`. Their kernel requests would hang if the ring left while they are held.
    outstanding: usize,
    /// Entries in state `Pending`, waiting for the ring thread to push their COMMIT_AND_FETCH.
    pending: Vec<u32>,
    /// An `-ENOTCONN` or `-ECONNABORTED` CQE was seen.
    conn_dead: bool,
    /// First unexpected CQE error or submit failure.
    fatal: Option<io::Error>,
    /// The session is tearing down. The kernel posts no CQE at unmount for an entry held in
    /// userspace, so a ring whose entries are all held would otherwise never see `conn_dead`.
    shutdown: bool,
    /// The session failed before it could serve, so the thread returns instead of draining.
    abandon: bool,
    /// Set in the same critical section that decides to exit, so a commit either finds it or
    /// is counted in `in_kernel` before the decision.
    exited: bool,
    /// CONSTELLATION PATCH (io-uring): per queue of this ring (`RingEntry::queue`), the
    /// entries a blocking lock request holds (`RingCommit::reserve_lock_wait`).
    lock_waits: Vec<u32>,
}

/// Decrements a `Live` count; a count already at zero means the accounting is wrong, which is
/// logged rather than wrapped so `Drop for Ring` keeps erring towards leaking.
fn dec(count: &mut usize, e: &RingEntry, what: &str) {
    match count.checked_sub(1) {
        Some(n) => *count = n,
        None => error!(
            "io_uring: qid {} entry {} left the {what} count while it was zero",
            e.qid, e.idx
        ),
    }
}

impl Live {
    /// Moves `state` to `Dead`, keeping `outstanding` in step with the state it leaves.
    fn kill(&mut self, e: &RingEntry, state: &mut EntryState) {
        if state.is_outstanding() {
            dec(&mut self.outstanding, e, "outstanding");
        }
        self.release_lock_wait(e);
        *state = EntryState::Dead;
    }

    /// CONSTELLATION PATCH (io-uring): `e` no longer holds its queue's lock-wait budget (its
    /// reply is committed, or it died); a no-op for an entry that never reserved it.
    fn release_lock_wait(&mut self, e: &RingEntry) {
        if e.lock_wait
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(n) = self.lock_waits.get_mut(e.queue as usize) {
                *n = n.saturating_sub(1);
            }
        }
    }
}

/// `libc::iovec` holds `*mut c_void`, so this newtype carries the `Send + Sync` impls.
/// Invariant: both iovecs point into `Ring::mem`, which outlives every SQE that names them.
struct EntryIov([libc::iovec; 2]);
// SAFETY: see the invariant above; the pointers are never dereferenced through this type.
unsafe impl Send for EntryIov {}
unsafe impl Sync for EntryIov {}

/// Start of an entry's stride in `Ring::mem`.
struct EntryPtr(NonNull<u8>);
// SAFETY: the pointee is plain memory in `Ring::mem`, accessed only under the entry state
// machine, which serializes writers.
unsafe impl Send for EntryPtr {}
unsafe impl Sync for EntryPtr {}

pub(crate) struct RingEntry {
    idx: u32,
    qid: u16,
    base: EntryPtr,
    gap: usize,
    payload_cap: usize,
    iov: EntryIov,
    state: Mutex<EntryState>,
    /// CONSTELLATION PATCH (io-uring): the handler of the current fetch took the request with
    /// `RingCommit::hold`, so the ring thread leaves the end of its dispatch to the
    /// `HeldRequest`. Only the ring thread reads or writes it, around and within a dispatch.
    held: std::sync::atomic::AtomicBool,
    /// CONSTELLATION PATCH (io-uring): the position of `qid` among this ring's queues, the
    /// index of its `Live::lock_waits` count.
    queue: u32,
    /// CONSTELLATION PATCH (io-uring): the current fetch counts against its queue's lock-wait
    /// budget (`RingCommit::reserve_lock_wait`); cleared, under `live`, when it stops holding
    /// the entry.
    lock_wait: std::sync::atomic::AtomicBool,
    /// CONSTELLATION PATCH (io-uring): the current fetch is a blocking lock request served as
    /// a non-blocking one (`HeldRequest::downgrade_lock_wait`), so a contended answer
    /// (`EAGAIN`) is committed as `ENOLCK`. Reset at every fetch.
    lock_downgraded: std::sync::atomic::AtomicBool,
    /// CONSTELLATION PATCH (io-uring): the REGISTER iovecs on a queue with a buffer pool: the
    /// header, and an empty payload, which the kernel requires there (it picks a pool buffer
    /// per request instead). Points into `Ring::mem` like `iov`.
    pool_iov: EntryIov,
    /// CONSTELLATION PATCH (io-uring): this entry's slot in the ring's buffer table, where the
    /// kernel registers a zero-copied request's pages (`POOL_BUF_INDEX` + 1 + index).
    zc_slot: u16,
    /// CONSTELLATION PATCH (io-uring): where the current fetch's reply payload goes -- the
    /// entry's own payload buffer, or the pool buffer the kernel picked for the request --
    /// and how many bytes it holds. The ring thread writes both at every fetch before the
    /// entry becomes `Dispatching`, so whoever commits read them after taking `state`.
    reply_at: AtomicPtr<u8>,
    reply_cap: AtomicUsize,
    /// CONSTELLATION PATCH (io-uring): the current fetch's pages are registered in `zc_slot`
    /// (`FUSE_URING_ENT_ZERO_COPY`), so its data reply reaches the caller only through a
    /// `READ_FIXED` into them: the kernel copies nothing from the payload buffer.
    zero_copied: AtomicBool,
    /// CONSTELLATION PATCH (io-uring): when the current fetch arrived (`mono_ms`; 0 before the
    /// first), its opcode and unique, for `Ring::report_held`. Written by the ring thread at
    /// every fetch, before the entry leaves the kernel's hands for userspace's.
    fetched_ms: std::sync::atomic::AtomicU64,
    fetched_opcode: std::sync::atomic::AtomicU32,
    fetched_unique: std::sync::atomic::AtomicU64,
    /// CONSTELLATION PATCH (io-uring): the unique `Ring::report_held` last logged, so an entry
    /// held across several watchdog passes is logged once per fetch.
    held_reported: std::sync::atomic::AtomicU64,
    /// CONSTELLATION PATCH (io-uring): when the entry was last queued in `Live::pending`
    /// (`mono_ms`), so the ring thread can tell a commit no wakeup announced from one whose
    /// wakeup is still on its way (`Ring::flush_stranded`).
    handed_ms: std::sync::atomic::AtomicU64,
}

/// CONSTELLATION PATCH (io-uring): milliseconds on a process-wide monotonic clock. It starts
/// at a day rather than at 0, which means "never" in the entry stamps, so a stamp can always be
/// set a while in the past (the tests do).
fn mono_ms() -> u64 {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    EPOCH.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64 + 86_400_000
}

/// CONSTELLATION PATCH (io-uring): the longest a ring thread waits for a completion before it
/// looks at its commit queue again on its own. Every reply handed to a ring thread from
/// another thread is announced through the wake eventfd, so this wait never ends by timing out
/// while the ring works as designed; it bounds what a lost announcement can cost -- a reply the
/// kernel's caller would otherwise wait for until the next request on one of this ring's
/// queues happened to wake the thread, which at the end of a workload is never. Entering the
/// kernel also runs whatever task work it queued for this thread (`DEFER_TASKRUN`), so a fetch
/// whose wake-up went missing is served by the same pass. One wake-up a second per idle ring.
const IDLE_WAIT: Duration = Duration::from_secs(1);

/// CONSTELLATION PATCH (io-uring): the wait once the eventfd poll is dead (`serve`).
const DEAD_WAKE_WAIT: Duration = Duration::from_millis(10);

/// CONSTELLATION PATCH (io-uring): a commit queued at least this long before an idle wait timed
/// out had its announcement lost; a younger one may still have its eventfd write under way
/// (the committer queues, then writes).
const STRANDED_AFTER_MS: u64 = 100;

#[derive(Debug)]
enum EntryState {
    /// A REGISTER (`last == 0`) or COMMIT_AND_FETCH for `last` is pending in the kernel.
    InKernel { last: u64 },
    /// The handler is running; `direct_ok` means the request has no payload, so a reply may
    /// be written while the request slice is live.
    Dispatching {
        direct_ok: bool,
        reply_taken: bool,
        commit_id: u64,
    },
    /// A reply arrived during dispatch while the payload was borrowed; written after dispatch.
    Deferred { reply: Stashed, commit_id: u64 },
    /// Dispatch returned, reply still to come.
    Dispatched { commit_id: u64 },
    /// Buffers being written by exactly one thread.
    Committing,
    /// Buffers written, `idx` is in `live.pending`, awaiting the ring thread's push.
    Pending { commit_id: u64 },
    /// CONSTELLATION PATCH (io-uring): `idx` is in `live.pending` for the ring thread to push
    /// a `READ_FIXED` into the request's registered pages; the commit follows its completion.
    /// Counted in `in_kernel` from here on, like `Pending`.
    PendingRead { commit_id: u64, read: ReadFixed },
    /// CONSTELLATION PATCH (io-uring): that `READ_FIXED` is in the kernel.
    Reading { commit_id: u64, read: ReadFixed },
    /// Not in the kernel and not coming back: before REGISTER, after ENOTCONN, a fatal
    /// error, or ring exit.
    Dead,
}

impl EntryState {
    /// Fetched and held by userspace, counted in `Live::outstanding`.
    fn is_outstanding(&self) -> bool {
        matches!(
            self,
            Self::Dispatching { .. }
                | Self::Deferred { .. }
                | Self::Dispatched { .. }
                | Self::Committing
        )
    }
}

/// A stashed reply; `Debug` prints only its length because the bytes are file data.
struct ReplyBytes(Vec<u8>);

impl fmt::Debug for ReplyBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0.len())
    }
}

/// CONSTELLATION PATCH (io-uring): a reply stashed during dispatch (`EntryState::Deferred`).
#[derive(Debug)]
enum Stashed {
    Bytes(ReplyBytes),
    Read(ReadFixed),
}

/// CONSTELLATION PATCH (io-uring): one `IORING_OP_READ_FIXED` of `len` bytes at `offset` of
/// `src` into a zero-copied request's registered pages (plan 38 §3(d)).
pub(crate) struct ReadFixed {
    src: ReadSource,
    offset: u64,
    len: u32,
}

/// CONSTELLATION PATCH (io-uring): where a `ReadFixed` reads from.
enum ReadSource {
    /// The filesystem's file (`ReplyData::read_fixed`), kept open until the read completes.
    Caller(Box<dyn AsFd + Send>),
    /// The ring's bounce memfd (`Ring::bounce_in`), whose range is released afterwards.
    Bounce,
}

impl fmt::Debug for ReadFixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let src = match &self.src {
            ReadSource::Caller(fd) => format!("fd {}", fd.as_fd().as_raw_fd()),
            ReadSource::Bounce => "bounce".to_owned(),
        };
        f.debug_struct("ReadFixed")
            .field("src", &src)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}

/// The ring variant of a reply sender. `Clone` so the usual owned reply objects can be
/// handed out; a second commit for the same fetch is rejected by the state machine, never
/// written.
#[derive(Clone)]
pub(crate) struct RingCommit {
    ring: Arc<Ring>,
    idx: u32,
    commit_id: u64,
}

impl fmt::Debug for RingCommit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingCommit")
            .field("ring", &self.ring.index)
            .field("idx", &self.idx)
            .field("commit_id", &self.commit_id)
            .finish()
    }
}

/// Outcome of `RingCommit::begin`.
enum Begun {
    /// The entry is `Committing`; write the buffers and hand off.
    Direct,
    /// The payload is borrowed: a reply given to `begin` was stored in the entry for the ring
    /// thread to write after dispatch; without one, the caller commits later.
    Deferred,
    /// The ring exited; nothing to do.
    Dropped,
}

/// Answers `EIO` and re-arms a `Committing` entry if the `fill` closure unwinds, so the
/// request fails instead of hanging; disarmed with `mem::forget` once the closure returned.
struct FillGuard<'a>(&'a RingCommit);

impl Drop for FillGuard<'_> {
    fn drop(&mut self) {
        warn!(
            "io_uring: reply closure for unique {} panicked; replying EIO",
            self.0.commit_id
        );
        self.0.write_errno_and_hand_off(Errno::EIO);
    }
}

impl RingCommit {
    fn entry(&self) -> &RingEntry {
        &self.ring.entries[self.idx as usize]
    }

    /// The `/dev/fuse` descriptor of the connection, for passthrough ioctls.
    pub(crate) fn device(&self) -> &Arc<DevFuse> {
        &self.ring.device
    }

    /// CONSTELLATION PATCH (io-uring): the transport this reply goes out over: the ring, with
    /// zero-copy when the session is (plan 38 Z4) -- every queue of every ring a zero-copy
    /// queue, as `RingSet::start` settled it, so that every reply of a session names the one
    /// transport the session reports. A ring outside a `RingSet` (the unit tests) answers for
    /// its own queues.
    pub(crate) fn transport(&self) -> crate::Transport {
        let zero_copy = self
            .ring
            .session_zero_copy
            .get()
            .copied()
            .unwrap_or_else(|| {
                self.ring
                    .pools
                    .get()
                    .is_some_and(|p| p.queues.iter().all(|q| q.zero_copy))
            });
        if zero_copy {
            crate::Transport::UringZeroCopy
        } else {
            crate::Transport::Uring
        }
    }

    /// CONSTELLATION PATCH (io-uring): whether this request's pages are registered for a
    /// `READ_FIXED` (`read_fixed`), i.e. a reply's data reaches them with no copy by this
    /// process.
    pub(crate) fn zero_copied(&self) -> bool {
        self.entry().zero_copied.load(Ordering::Relaxed)
    }

    /// `live` is released before the reply is copied; `hand_off` re-checks `exited` under both
    /// locks. A refused commit is `NotConnected` when the connection ended (expected after
    /// unmount) and `Other` for a duplicate (a filesystem bug). `iov` is stashed when the
    /// payload is borrowed; `None` leaves the state alone then.
    ///
    /// CONSTELLATION PATCH (io-uring): so is `read`, a `READ_FIXED` reply (`read_fixed`),
    /// taken out of the option when it is stashed.
    fn begin(
        &self,
        iov: Option<&[IoSlice<'_>]>,
        read: &mut Option<ReadFixed>,
    ) -> io::Result<Begun> {
        let e = self.entry();
        let mut state = e.state.lock();
        let (exited, conn_dead) = {
            let live = self.ring.live.lock();
            (live.exited, live.conn_dead)
        };
        if exited {
            self.ring.live.lock().kill(e, &mut state);
            drop(state);
            debug!(
                "io_uring: dropping reply for unique {} after ring {} exited",
                self.commit_id, self.ring.index
            );
            return Ok(Begun::Dropped);
        }
        match &*state {
            EntryState::Dispatching {
                direct_ok: true,
                commit_id,
                ..
            }
            | EntryState::Dispatched { commit_id }
                if *commit_id == self.commit_id =>
            {
                *state = EntryState::Committing;
                Ok(Begun::Direct)
            }
            EntryState::Dispatching {
                direct_ok: false,
                commit_id,
                ..
            } if *commit_id == self.commit_id => {
                if let Some(iov) = iov {
                    let mut bytes = Vec::with_capacity(iov.iter().map(|s| s.len()).sum());
                    iov.iter().for_each(|s| bytes.extend_from_slice(s));
                    *state = EntryState::Deferred {
                        reply: Stashed::Bytes(ReplyBytes(bytes)),
                        commit_id: self.commit_id,
                    };
                } else if let Some(read) = read.take() {
                    *state = EntryState::Deferred {
                        reply: Stashed::Read(read),
                        commit_id: self.commit_id,
                    };
                }
                Ok(Begun::Deferred)
            }
            _ if conn_dead => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "reply after the connection ended",
            )),
            _ => Err(io::Error::other("duplicate reply")),
        }
    }

    /// Commits a reply; `iov[0]` is the `fuse_out_header`, the rest the payload. Anything else
    /// in `iov[0]` is answered with `EINVAL`, whichever path the reply takes.
    pub(crate) fn commit(&self, iov: &[IoSlice<'_>]) -> io::Result<()> {
        if iov.first().is_none_or(|h| h.len() != OUT_HEADER_SZ) {
            error!(
                "io_uring: reply for unique {} does not start with a fuse_out_header; replying \
                 EINVAL",
                self.commit_id
            );
            let header = errno_header(self.commit_id, Errno::EINVAL);
            return self.commit(&[IoSlice::new(header.as_bytes())]);
        }
        if self
            .entry()
            .lock_downgraded
            .load(std::sync::atomic::Ordering::Relaxed)
            && iov[0][4..8] == (-Errno::EAGAIN.code()).to_ne_bytes()
        {
            // A blocking lock request that was not allowed to wait on this queue
            // (`HeldRequest::downgrade_lock_wait`): "would block" is not an answer `F_SETLKW` or
            // a blocking `flock` may give, "no lock resources" is
            let header = errno_header(self.commit_id, Errno::ENOLCK);
            return self.commit_unmapped(&[IoSlice::new(header.as_bytes())]);
        }
        self.commit_unmapped(iov)
    }

    fn commit_unmapped(&self, iov: &[IoSlice<'_>]) -> io::Result<()> {
        if let Begun::Direct = self.begin(Some(iov), &mut None)? {
            // `Committing` makes this thread the only writer; a request slice can only be
            // live if the request had no payload, and it never covers the header or the
            // payload area.
            self.ring.deliver(self.entry(), self.commit_id, iov);
        }
        Ok(())
    }

    /// CONSTELLATION PATCH (io-uring): replies with `len` bytes of `src` from `offset` (plan 38
    /// §3(d)). On a zero-copied request -- a read of a file opened with
    /// `FOPEN_IO_URING_ZERO_COPY` on a zero-copy queue -- the ring thread issues one
    /// `IORING_OP_READ_FIXED` from `src` straight into the request's registered pages, and
    /// commits once it completes: the bytes it read (fewer at the end of the file) or its
    /// error. Otherwise the bytes are read into the entry's payload buffer with `pread(2)`, as
    /// `fill_with` writes any reply there. `Ok(Some(src))` hands `src` back when the request's
    /// payload is borrowed and it was not zero-copied, as `fill_with` hands back its closure;
    /// `Ok(None)` means the reply is done or under way. `src` stays open until the read
    /// completed, on the ring thread.
    pub(crate) fn read_fixed(
        &self,
        src: Box<dyn AsFd + Send>,
        offset: u64,
        len: usize,
    ) -> io::Result<Option<Box<dyn AsFd + Send>>> {
        let e = self.entry();
        if !e.zero_copied.load(Ordering::Relaxed) {
            let back = self.fill_with(len, false, |buf| {
                pread_full(src.as_fd(), buf, offset).map_err(Errno::from)
            })?;
            return Ok(back.map(drop).map(|()| src));
        }
        let Ok(len) = u32::try_from(len) else {
            let header = errno_header(self.commit_id, Errno::EINVAL);
            return self
                .commit(&[IoSlice::new(header.as_bytes())])
                .map(|()| None);
        };
        let mut read = Some(ReadFixed {
            src: ReadSource::Caller(src),
            offset,
            len,
        });
        match self.begin(None, &mut read)? {
            // A held request stashed the read for `finish_dispatch`
            Begun::Dropped | Begun::Deferred => Ok(None),
            Begun::Direct => {
                if let Some(read) = read {
                    self.ring.hand_off_read(e, self.commit_id, read);
                }
                Ok(None)
            }
        }
    }

    /// `fill_with` with a zeroed buffer, for the tests (replies go through `fill_with`).
    #[cfg(test)]
    pub(crate) fn fill<F>(&self, max_len: usize, f: F) -> io::Result<Option<F>>
    where
        F: FnOnce(&mut [u8]) -> Result<usize, Errno>,
    {
        self.fill_with(max_len, true, f)
    }

    /// Commits a reply whose payload `f` writes into the entry's payload buffer itself.
    /// `Ok(None)` means the reply is done (written, dropped, or answered `EINVAL`);
    /// `Ok(Some(f))` hands the closure back because the request's payload is borrowed, and
    /// the caller commits a heap buffer through `commit` instead.
    ///
    /// CONSTELLATION PATCH (io-uring): `zero: false` skips zeroing the buffer handed to `f`.
    /// The gather form writes every byte it reports, so the memset a closure that might not
    /// is owed would only double the work of the copy it precedes. The bytes `f` sees are
    /// then whatever the entry held (an earlier request or reply, or the mapping's zeros):
    /// initialized memory, never anything outside this entry.
    pub(crate) fn fill_with<F>(&self, max_len: usize, zero: bool, f: F) -> io::Result<Option<F>>
    where
        F: FnOnce(&mut [u8]) -> Result<usize, Errno>,
    {
        let e = self.entry();
        if max_len > e.reply_cap() {
            error!(
                "io_uring: reply buffer of {max_len} bytes for unique {} exceeds the {} byte \
                 payload buffer; replying EINVAL",
                self.commit_id,
                e.reply_cap()
            );
            let header = errno_header(self.commit_id, Errno::EINVAL);
            return self
                .commit(&[IoSlice::new(header.as_bytes())])
                .map(|()| None);
        }
        match self.begin(None, &mut None)? {
            Begun::Dropped => Ok(None),
            Begun::Deferred => Ok(Some(f)),
            Begun::Direct => {
                #[cfg(test)]
                self.ring
                    .hooks
                    .direct_fills
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let guard = FillGuard(self);
                // CONSTELLATION PATCH (io-uring): a zero-copied request's data can only reach
                // its pages through a `READ_FIXED`, so `f` writes into the entry's own payload
                // buffer -- unused by a request that had no payload -- and the bytes are
                // bounced from there (`Ring::bounce_in`)
                let zero_copied = e.zero_copied.load(Ordering::Relaxed);
                let at = if zero_copied {
                    e.own_payload()
                } else {
                    e.reply_payload()
                };
                // SAFETY: `Committing` makes this thread the only writer, and a request slice
                // can only be live if the request had no payload, so it ends before the
                // payload area; `max_len <= reply_cap <= payload_cap` keeps the slice inside
                // either buffer.
                let res = unsafe { e.with_payload(at, max_len, zero, f) };
                std::mem::forget(guard);
                if zero_copied {
                    match self.checked(res, max_len) {
                        Ok(n) => {
                            // SAFETY: `f` is done with the buffer; `n <= max_len`.
                            let bytes = unsafe { slice::from_raw_parts(at, n as usize) };
                            let header = abi::fuse_out_header {
                                len: OUT_HEADER_SZ as u32 + n,
                                error: 0,
                                unique: self.commit_id,
                            };
                            self.ring.deliver(
                                e,
                                self.commit_id,
                                &[IoSlice::new(header.as_bytes()), IoSlice::new(bytes)],
                            );
                        }
                        Err(errno) => self.write_errno_and_hand_off(errno),
                    }
                    return Ok(None);
                }
                match self.checked(res, max_len) {
                    Ok(n) => {
                        let header = abi::fuse_out_header {
                            len: OUT_HEADER_SZ as u32 + n,
                            error: 0,
                            unique: self.commit_id,
                        };
                        // SAFETY: as above; `checked` bounded `n` by the payload.
                        unsafe { e.write_header(header.as_bytes(), n) }
                    }
                    // SAFETY: as above.
                    Err(errno) => unsafe { e.write_errno(self.commit_id, errno) },
                }
                self.ring.hand_off(e, self.commit_id);
                Ok(None)
            }
        }
    }

    /// A `fill` closure's count as the trailer's `payload_sz`; one beyond the buffer, or
    /// beyond what `fuse_out_header.len` can name, is a caller bug answered with `EINVAL`.
    fn checked(&self, res: Result<usize, Errno>, len: usize) -> Result<u32, Errno> {
        let n = res?;
        u32::try_from(n)
            .ok()
            .filter(|sz| n <= len && sz.checked_add(OUT_HEADER_SZ as u32).is_some())
            .ok_or_else(|| {
                error!(
                    "io_uring: reply for unique {} claims {n} bytes in a {len} byte buffer; \
                     replying EINVAL",
                    self.commit_id
                );
                Errno::EINVAL
            })
    }

    /// Commits an errno reply. Only valid while the entry is `Dispatching` or `Dispatched`;
    /// anything else is logged and ignored.
    pub(crate) fn commit_errno(&self, errno: Errno) {
        let header = errno_header(self.commit_id, errno);
        match self.commit(&[IoSlice::new(header.as_bytes())]) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotConnected => {
                debug!("io_uring: {err} for unique {}", self.commit_id)
            }
            Err(err) => error!(
                "io_uring: {err} for unique {} on qid {}",
                self.commit_id,
                self.entry().qid
            ),
        }
    }

    /// Errno reply for an entry that is already `Committing`.
    fn write_errno_and_hand_off(&self, errno: Errno) {
        // SAFETY: the caller made the entry `Committing`, so this thread is its only writer.
        unsafe { self.entry().write_errno(self.commit_id, errno) };
        self.ring.hand_off(self.entry(), self.commit_id);
    }

    /// CONSTELLATION PATCH (io-uring): counts this fetch -- a blocking lock request -- against
    /// its queue's lock-wait budget, if the budget allows: at most `depth - 1` entries of one
    /// queue may be held by blocking lock requests, so one entry of every queue is always left
    /// for requests that do not wait on another process.
    ///
    /// Why: the kernel queues a request on the queue of the CPU that submitted it and lets it
    /// wait there until one of that queue's entries is committed; it cannot be answered on any
    /// other entry, any other queue or `/dev/fuse`. A blocking lock request holds its entry
    /// until the lock is granted, which may wait on the holder's next request -- a `write()`
    /// before its unlock. Were all `depth` entries of the holder's CPU's queue held by such
    /// waiters, that request, and with it the unlock, would never be served: a deadlock no
    /// `/dev/fuse` session has, where a waiting request holds nothing. `false` means the
    /// budget is spent; the caller then serves the request without waiting
    /// (`HeldRequest::downgrade_lock_wait`).
    pub(crate) fn reserve_lock_wait(&self) -> bool {
        let e = self.entry();
        let mut live = self.ring.live.lock();
        let budget = self.ring.depth.saturating_sub(1);
        let Some(n) = live.lock_waits.get_mut(e.queue as usize) else {
            return false;
        };
        if *n >= budget {
            return false;
        }
        *n += 1;
        e.lock_wait
            .store(true, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// CONSTELLATION PATCH (io-uring): takes the fetched request off the ring thread.
    ///
    /// Called by a `FetchHandler` during `handle`, on the ring thread, with the request slice
    /// it was given. The ring thread then returns to its queues at once instead of finishing
    /// the dispatch: the entry stays fetched -- out of the kernel, its buffers untouched by it
    /// -- and the returned `HeldRequest` carries the request to whichever thread dispatches
    /// it, and finishes the dispatch there when it is dropped, exactly as the ring thread
    /// would have. This is what keeps a callback that blocks (a flush waiting on a remote
    /// store) from stalling every other request the kernel queues on this ring.
    pub(crate) fn hold(self, request: &[u8]) -> HeldRequest {
        let e = self.entry();
        debug_assert_eq!(
            self.ring.ring_thread.get(),
            Some(&thread::current().id()),
            "a fetch is held by its ring thread, while it dispatches"
        );
        let base = e.base.0.as_ptr() as usize;
        let start = request.as_ptr() as usize;
        assert!(
            start >= base && start + request.len() <= base + e.gap + e.payload_cap,
            "a held request lies in its own entry"
        );
        // A reply made while the request is held is stashed, never written at once, even for a
        // request without a payload: a direct reply re-arms the entry, and the next fetch
        // into it would be staged over the bytes the held slice still covers
        match &mut *e.state.lock() {
            EntryState::Dispatching {
                direct_ok,
                commit_id,
                ..
            } if *commit_id == self.commit_id => *direct_ok = false,
            other => panic!(
                "io_uring: entry {} is {other:?} when its fetch is held; a handler holds a \
                 fetch before anything replies to it",
                e.idx
            ),
        }
        e.held.store(true, std::sync::atomic::Ordering::Relaxed);
        HeldRequest {
            req: EntryPtr(NonNull::from(request).cast()),
            len: request.len(),
            commit: self,
            rewritten: None,
        }
    }

    /// Records that a reply object exists for this fetch, so the ring thread does not answer
    /// with an empty reply when dispatch returns without one.
    pub(crate) fn reply_created(&self) {
        if let EntryState::Dispatching {
            reply_taken,
            commit_id,
            ..
        } = &mut *self.entry().state.lock()
        {
            if *commit_id == self.commit_id {
                *reply_taken = true;
            }
        }
    }
}

/// CONSTELLATION PATCH (io-uring): a fetched request whose dispatch continues off the ring
/// thread (`RingCommit::hold`).
///
/// **The borrow invariant, upheld here and only here.** `request()` is a slice of the entry's
/// own stride -- the staged header and, for a `FUSE_WRITE` or `FUSE_SETXATTR`, the payload the
/// kernel copied in, so a write's bytes reach the filesystem with no copy at all. It stays
/// valid exactly as long as this value: the entry cannot be re-armed (`COMMIT_AND_FETCH`)
/// before `Drop` has run `Ring::finish_dispatch`, because until then the entry is
/// `Dispatching` with `direct_ok` cleared by `hold` (counted `outstanding`, never `Pending`), so
/// a reply made meanwhile, from any thread, is stashed (`Deferred`) and written by that
/// `finish_dispatch`, after the last use of the slice. `request()` borrows `self`, so
/// no slice can outlive the drop; and the `Arc<Ring>` in `commit` keeps the mapping alive even
/// if the ring thread has exited meanwhile.
///
/// Holding takes the request off the ring *thread*, not off its *entry*: the entry stays
/// fetched until the request is answered, however late and from whatever thread, and while
/// every entry of a queue is fetched the kernel holds that CPU's further requests back. A
/// request whose answer waits on another request from the same CPU can therefore deadlock
/// the queue; for blocking locks, the one such request a filesystem has, see
/// `RingCommit::reserve_lock_wait`.
pub(crate) struct HeldRequest {
    commit: RingCommit,
    req: EntryPtr,
    len: usize,
    /// A rewritten copy of the request, served instead of the entry's
    /// (`downgrade_lock_wait`).
    rewritten: Option<Box<[u8]>>,
}

impl HeldRequest {
    /// The handle a reply to this request commits through (it may be cloned and outlive this).
    pub(crate) fn commit(&self) -> &RingCommit {
        &self.commit
    }

    /// The contiguous request, as the ring thread's handler was given it.
    pub(crate) fn request(&self) -> &[u8] {
        if let Some(rewritten) = &self.rewritten {
            return rewritten;
        }
        // SAFETY: see the type's doc: `[req, req + len)` is inside the entry and nothing writes
        // it until `Drop` finishes the dispatch, which `&self` outlives.
        unsafe { slice::from_raw_parts(self.req.0.as_ptr(), self.len) }
    }

    /// CONSTELLATION PATCH (io-uring): serves this `FUSE_SETLKW` as a `FUSE_SETLK`, because
    /// its queue's lock-wait budget is spent (`RingCommit::reserve_lock_wait`): granted at
    /// once if the lock is free, and a conflict answered `ENOLCK` instead of waiting on an
    /// entry the queue cannot spare. The request is small (a header and a `fuse_lk_in`), so it
    /// is copied rather than rewritten in the entry.
    pub(crate) fn downgrade_lock_wait(&mut self) {
        let mut copy: Box<[u8]> = self.request().into();
        if let Some(opcode) = copy.get_mut(4..8) {
            opcode.copy_from_slice(&(abi::fuse_opcode::FUSE_SETLK as u32).to_ne_bytes());
        }
        self.rewritten = Some(copy);
        self.commit
            .entry()
            .lock_downgraded
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(hook) = self.commit.ring.lock_wait_downgrades.get() {
            hook.notify();
        }
    }
}

impl fmt::Debug for HeldRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldRequest")
            .field("commit", &self.commit)
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for HeldRequest {
    fn drop(&mut self) {
        let e = self.commit.entry();
        self.commit
            .ring
            .finish_dispatch(e, self.commit.commit_id, true);
    }
}

fn errno_header(unique: u64, errno: Errno) -> abi::fuse_out_header {
    abi::fuse_out_header {
        len: OUT_HEADER_SZ as u32,
        error: -errno.code(),
        unique,
    }
}

impl RingEntry {
    /// Writes a reply: header into `[0, 16)`, payload into `[gap, ..)`, then the trailer. A
    /// payload larger than the buffer, or `iov[0]` that is not a `fuse_out_header`, becomes an
    /// `EINVAL` reply, as `/dev/fuse` would make an oversized one. CONSTELLATION PATCH
    /// (io-uring): on a pool queue "the buffer" is `reply_cap`, 0 for a request whose reply the
    /// kernel reads no payload from, which may have been given no pool buffer of its own
    /// (`Ring::locate_payload`).
    ///
    /// # Safety
    ///
    /// The caller is the entry's only writer (state `Committing`, or the ring thread right
    /// after a CQE) and no reference into the header or payload area is live.
    unsafe fn write_reply(&self, commit_id: u64, iov: &[IoSlice<'_>]) {
        let payload_len: usize = iov.iter().skip(1).map(|s| s.len()).sum();
        let header = iov.first().filter(|h| h.len() == OUT_HEADER_SZ);
        let payload_sz = u32::try_from(payload_len)
            .ok()
            .filter(|_| payload_len <= self.reply_cap());
        // `commit` already answered a malformed header, so only the payload can fail here
        let (Some(header), Some(payload_sz)) = (header, payload_sz) else {
            error!(
                "io_uring: reply of {payload_len} bytes exceeds the {} byte payload buffer; \
                 replying EINVAL",
                self.reply_cap()
            );
            // SAFETY: the caller's guarantee.
            unsafe { self.write_errno(commit_id, Errno::EINVAL) };
            return;
        };
        let at = self.reply_payload();
        // SAFETY: the payload fits in `reply_cap`, so every chunk lands inside the reply
        // buffer (the stride's payload, or the pool buffer the kernel picked).
        unsafe {
            let mut off = 0;
            for chunk in iov.iter().skip(1) {
                ptr::copy_nonoverlapping(chunk.as_ptr(), at.add(off), chunk.len());
                off += chunk.len();
            }
            self.write_header(header, payload_sz);
        }
    }

    /// Writes the out header into `[0, 16)` and the trailer naming `payload_sz` bytes that
    /// are already in the payload area.
    ///
    /// # Safety
    ///
    /// As for `write_reply`, and `payload_sz <= payload_cap`.
    unsafe fn write_header(&self, header: &[u8], payload_sz: u32) {
        assert_eq!(header.len(), OUT_HEADER_SZ);
        let base = self.base.0.as_ptr();
        // SAFETY: the header and the trailer fields lie inside the entry's header area.
        unsafe {
            ptr::copy_nonoverlapping(header.as_ptr(), base, OUT_HEADER_SZ);
            ptr::write_unaligned(base.add(FLAGS_OFFSET).cast::<u64>(), 0);
            ptr::write_unaligned(base.add(PAYLOAD_SZ_OFFSET).cast::<u32>(), payload_sz);
        }
    }

    /// Runs `f` on the first `len` bytes of the payload buffer at `at`, zeroed when `zero`.
    ///
    /// CONSTELLATION PATCH (io-uring): `at` is `reply_payload()` or `own_payload()`.
    ///
    /// # Safety
    ///
    /// As for `write_reply`, `at` is one of this entry's payload buffers, `len` is at most
    /// what that buffer holds, and no other reference into it exists while `f` runs.
    unsafe fn with_payload<R>(
        &self,
        at: *mut u8,
        len: usize,
        zero: bool,
        f: impl FnOnce(&mut [u8]) -> R,
    ) -> R {
        // SAFETY: `[at, at + len)` is inside the buffer and, by the caller's guarantee,
        // aliased by nothing else while the slice is live. Unzeroed, its bytes are still
        // initialized: both mappings are anonymous memory, written since only by the kernel
        // and this ring's own requests and replies.
        let buf = unsafe {
            if zero {
                ptr::write_bytes(at, 0, len);
            }
            slice::from_raw_parts_mut(at, len)
        };
        f(buf)
    }

    /// CONSTELLATION PATCH (io-uring): the entry's own payload buffer in its stride, where a
    /// fetched request's payload is staged to follow its header.
    fn own_payload(&self) -> *mut u8 {
        // SAFETY: gap < stride, inside the entry's stride.
        unsafe { self.base.0.as_ptr().add(self.gap) }
    }

    /// CONSTELLATION PATCH (io-uring): where the current fetch's reply payload goes
    /// (`reply_at`).
    fn reply_payload(&self) -> *mut u8 {
        self.reply_at.load(Ordering::Relaxed)
    }

    /// CONSTELLATION PATCH (io-uring): how many bytes a reply payload of the current fetch
    /// may have (`reply_cap`).
    fn reply_cap(&self) -> usize {
        self.reply_cap.load(Ordering::Relaxed)
    }

    /// # Safety
    ///
    /// As for `write_reply`.
    unsafe fn write_errno(&self, commit_id: u64, errno: Errno) {
        let header = errno_header(commit_id, errno);
        // SAFETY: the caller's guarantee; a header-only iov always passes the size checks.
        unsafe { self.write_reply(commit_id, &[IoSlice::new(header.as_bytes())]) };
    }
}

/// `fuse_uring_cmd_req` at the front of the 80-byte area, the rest zero for 7.46.
fn cmd_bytes(qid: u16, commit_id: u64) -> [u8; 80] {
    let req = abi::fuse_uring_cmd_req {
        flags: 0,
        commit_id,
        qid,
        padding: [0; 6],
    };
    let mut cmd = [0u8; 80];
    cmd[..size_of::<abi::fuse_uring_cmd_req>()].copy_from_slice(req.as_bytes());
    cmd
}

/// `(qid, entry index)` to `user_data`; the index is the position in `Ring::entries`.
fn user_data(qid: u16, idx: u32) -> u64 {
    (u64::from(qid) << 32) | u64::from(idx)
}

fn decode(ud: u64) -> (u16, u32) {
    ((ud >> 32) as u16, ud as u32)
}

/// `opcode::UringCmd80` has no `len` setter, and REGISTER needs `len == 2` (the iovec count).
fn set_sqe_len(sqe: squeue::Entry128, len: u32) -> squeue::Entry128 {
    // SAFETY: Entry128 is repr(C), 128 bytes, and every bit pattern of its integer fields is
    // valid, so a round trip through a byte array to patch one field is sound.
    let mut raw: [u8; 128] = unsafe { std::mem::transmute(sqe) };
    raw[SQE_LEN_OFFSET..SQE_LEN_OFFSET + 4].copy_from_slice(&len.to_ne_bytes());
    // SAFETY: as above.
    unsafe { std::mem::transmute(raw) }
}

/// `io_uring_enter` errors that mean the ring itself is unusable, as opposed to one SQE.
fn is_ring_failure(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EBADF | libc::ENXIO | libc::EFAULT)
    )
}

impl Ring {
    /// Reserves the buffers and the wake eventfd for `depth` entries on each of `qids`.
    pub(crate) fn new(
        index: usize,
        mounted: bool,
        device: Arc<DevFuse>,
        qids: &[u16],
        depth: u32,
        payload_cap: usize,
    ) -> io::Result<Arc<Ring>> {
        let n = qids
            .len()
            .checked_mul(depth as usize)
            .filter(|n| *n > 0 && u32::try_from(*n).is_ok())
            .ok_or_else(|| io::Error::other("io_uring: invalid entry count"))?;
        let mem = RingMemory::new(n, payload_cap)?;
        let wake = EventFd::from_value_and_flags(0, EfdFlags::EFD_CLOEXEC | EfdFlags::EFD_NONBLOCK)
            .map_err(|err| io::Error::other(format!("creating the wake eventfd failed ({err})")))?;
        let entries = qids
            .iter()
            .flat_map(|&qid| std::iter::repeat_n(qid, depth as usize))
            .enumerate()
            .map(|(idx, qid)| {
                let base = mem.entry(idx);
                let iov = [
                    libc::iovec {
                        iov_base: base.as_ptr().cast(),
                        iov_len: HEADER_SZ,
                    },
                    libc::iovec {
                        // SAFETY: gap < stride, inside the entry's stride.
                        iov_base: unsafe { base.add(mem.gap()) }.as_ptr().cast(),
                        iov_len: mem.payload_cap(),
                    },
                ];
                let pool_iov = [
                    iov[0],
                    libc::iovec {
                        iov_base: ptr::null_mut(),
                        iov_len: 0,
                    },
                ];
                RingEntry {
                    idx: idx as u32,
                    qid,
                    base: EntryPtr(base),
                    gap: mem.gap(),
                    payload_cap: mem.payload_cap(),
                    iov: EntryIov(iov),
                    state: Mutex::new(EntryState::Dead),
                    held: std::sync::atomic::AtomicBool::new(false),
                    queue: idx as u32 / depth,
                    lock_wait: std::sync::atomic::AtomicBool::new(false),
                    lock_downgraded: std::sync::atomic::AtomicBool::new(false),
                    pool_iov: EntryIov(pool_iov),
                    // `n` fits a u32 and is at most 32768 per ring (`RingSet::new`); a ring
                    // with more entries than a buffer table has slots never asks for zero-copy
                    zc_slot: (idx + 1).min(usize::from(u16::MAX)) as u16 + POOL_BUF_INDEX,
                    // SAFETY: gap < stride, inside the entry's stride.
                    reply_at: AtomicPtr::new(unsafe { base.add(mem.gap()) }.as_ptr()),
                    reply_cap: AtomicUsize::new(mem.payload_cap()),
                    zero_copied: AtomicBool::new(false),
                    fetched_ms: std::sync::atomic::AtomicU64::new(0),
                    fetched_opcode: std::sync::atomic::AtomicU32::new(0),
                    fetched_unique: std::sync::atomic::AtomicU64::new(0),
                    held_reported: std::sync::atomic::AtomicU64::new(0),
                    handed_ms: std::sync::atomic::AtomicU64::new(0),
                }
            })
            .collect();
        Ok(Arc::new(Ring {
            index,
            depth,
            mounted,
            device,
            ring_thread: OnceLock::new(),
            wake,
            live: Mutex::new(Live {
                in_kernel: 0,
                outstanding: 0,
                pending: Vec::new(),
                conn_dead: false,
                fatal: None,
                shutdown: false,
                abandon: false,
                exited: false,
                lock_waits: vec![0; qids.len()],
            }),
            entries,
            mem: ManuallyDrop::new(mem),
            malformed_register: std::sync::atomic::AtomicBool::new(false),
            lock_wait_downgrades: OnceLock::new(),
            health: OnceLock::new(),
            stranded_logged: std::sync::atomic::AtomicU64::new(0),
            pool_plan: Mutex::new(None),
            pools: OnceLock::new(),
            setup_gate: Mutex::new(None),
            session_zero_copy: OnceLock::new(),
            bounce: OnceLock::new(),
            #[cfg(test)]
            hooks: test::RingHooks::default(),
        }))
    }

    /// CONSTELLATION PATCH (io-uring): asks for zero-copy queues (plan 38 Z4), with `pool` --
    /// one slice of `depth` buffers per queue of this ring -- as their buffer pools, registered
    /// as a fixed buffer when `register`. `set_up_table` and `set_up_queues` decide, before the
    /// first REGISTER.
    pub(crate) fn reserve_pools(&self, pool: PoolMemory, register: bool) {
        *self.pool_plan.lock() = Some((pool, register));
    }

    /// CONSTELLATION PATCH (io-uring): see `Ring::setup_gate`.
    pub(crate) fn set_setup_gate(&self, gate: SetupGate) {
        *self.setup_gate.lock() = Some(gate);
    }

    /// CONSTELLATION PATCH (io-uring): see `Ring::session_zero_copy`.
    pub(crate) fn set_session_zero_copy(&self, zero_copy: bool) {
        let _ = self.session_zero_copy.set(zero_copy);
    }

    /// CONSTELLATION PATCH (io-uring): address space reserved for buffer pools, if any.
    pub(crate) fn pool_bytes(&self) -> usize {
        self.pool_plan
            .lock()
            .as_ref()
            .map(|(pool, _)| pool.len())
            .or_else(|| self.pools.get().map(|p| p.mem.len()))
            .unwrap_or(0)
    }

    /// CONSTELLATION PATCH (io-uring): this ring's queue `q` (by `RingEntry::queue`), as its
    /// pool setup left it; `None` when its entries have their own payload buffers.
    fn queue_pool(&self, q: u32) -> Option<QueuePool> {
        self.pools
            .get()
            .and_then(|p| p.queues.get(q as usize).copied())
    }

    /// How many `fill` replies were written straight into an entry.
    #[cfg(test)]
    pub(crate) fn direct_fills(&self) -> usize {
        self.hooks
            .direct_fills
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Address space reserved for the entry buffers.
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.mem.len()
    }

    /// Tells the ring thread to leave once nothing is pending in the kernel, even while
    /// requests are still held by userspace; call after the connection ended.
    pub(crate) fn shutdown(&self) {
        self.live.lock().shutdown = true;
        if let Err(err) = self.wake.write(1) {
            error!("io_uring: eventfd write failed: {err}");
        }
    }

    /// Makes a thread still waiting for its handler return, as a dropped `from_fd` session's
    /// does, instead of serving until the connection ends. For a session that failed to start,
    /// mounted or not: a caller blocked in the mount keeps its unmount from ending the
    /// connection, so the registered commands must be cancelled for it to abort.
    pub(crate) fn abandon(&self) {
        self.live.lock().abandon = true;
    }

    fn register_sqe(&self, e: &RingEntry) -> squeue::Entry128 {
        // CONSTELLATION PATCH (io-uring): on a queue with a pool the payload iovec is empty,
        // a zero-copy queue's entry names its buffer-table slot, and a registered pool's
        // index goes with every command of its queue
        let pool = self.queue_pool(e.queue);
        let mut cmd = cmd_bytes(e.qid, 0);
        if pool.is_some_and(|q| q.zero_copy) {
            let at = abi::FUSE_URING_CMD_REQ_UNION_OFFSET;
            cmd[at..at + 2].copy_from_slice(&e.zc_slot.to_ne_bytes());
        }
        let iov = if pool.is_some() { &e.pool_iov } else { &e.iov };
        let sqe = opcode::UringCmd80::new(
            types::Fd(self.device.as_raw_fd()),
            abi::fuse_uring_cmd::FUSE_IO_URING_CMD_REGISTER as u32,
        )
        .cmd(cmd)
        .addr(Some(iov.0.as_ptr() as u64))
        .buf_index(pool.filter(|q| q.fixed).map(|_| POOL_BUF_INDEX))
        .build()
        .user_data(user_data(e.qid, e.idx));
        let segments = if self
            .malformed_register
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            1
        } else {
            2
        };
        set_sqe_len(sqe, segments)
    }

    /// CONSTELLATION PATCH (io-uring): see `Ring::malformed_register`.
    pub(crate) fn set_malformed_register(&self) {
        self.malformed_register
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// CONSTELLATION PATCH (io-uring): see `Ring::lock_wait_downgrades`.
    pub(crate) fn set_lock_wait_downgrades(&self, hook: super::LockWaitDowngrades) {
        let _ = self.lock_wait_downgrades.set(hook);
    }

    /// CONSTELLATION PATCH (io-uring): see `Ring::health`.
    pub(crate) fn set_health(&self, health: super::RingHealth) {
        let _ = self.health.set(health);
    }

    fn commit_sqe(&self, e: &RingEntry, commit_id: u64) -> squeue::Entry128 {
        opcode::UringCmd80::new(
            types::Fd(self.device.as_raw_fd()),
            abi::fuse_uring_cmd::FUSE_IO_URING_CMD_COMMIT_AND_FETCH as u32,
        )
        .cmd(cmd_bytes(e.qid, commit_id))
        // CONSTELLATION PATCH (io-uring): see `register_sqe`
        .buf_index(
            self.queue_pool(e.queue)
                .filter(|q| q.fixed)
                .map(|_| POOL_BUF_INDEX),
        )
        .build()
        .user_data(user_data(e.qid, e.idx))
    }

    /// CONSTELLATION PATCH (io-uring): `FUSE_IO_URING_CMD_ADD_QUEUE` for `qid`, a zero-copy
    /// queue when `zero_copy`.
    fn add_queue_sqe(&self, qid: u16, zero_copy: bool) -> squeue::Entry128 {
        let mut cmd = cmd_bytes(qid, 0);
        if zero_copy {
            cmd[..8].copy_from_slice(&abi::FUSE_URING_ZERO_COPY.to_ne_bytes());
        }
        opcode::UringCmd80::new(
            types::Fd(self.device.as_raw_fd()),
            abi::fuse_uring_cmd::FUSE_IO_URING_CMD_ADD_QUEUE as u32,
        )
        .cmd(cmd)
        .build()
    }

    /// CONSTELLATION PATCH (io-uring): `FUSE_IO_URING_CMD_ADD_BUFPOOL` giving `qid` the pool
    /// `[start, start + len)`, reached through the fixed buffer at `POOL_BUF_INDEX` when
    /// `fixed` (the kernel takes the SQE's `IORING_URING_CMD_FIXED` and `buf_index` as that).
    fn add_bufpool_sqe(
        &self,
        qid: u16,
        start: NonNull<u8>,
        len: usize,
        fixed: bool,
    ) -> squeue::Entry128 {
        let mut cmd = cmd_bytes(qid, 0);
        let pool = abi::fuse_uring_bufpool {
            uaddr: start.as_ptr() as u64,
            len: len as u32,
            reserved: 0,
        };
        let at = abi::FUSE_URING_CMD_REQ_UNION_OFFSET;
        cmd[at..at + size_of::<abi::fuse_uring_bufpool>()].copy_from_slice(pool.as_bytes());
        opcode::UringCmd80::new(
            types::Fd(self.device.as_raw_fd()),
            abi::fuse_uring_cmd::FUSE_IO_URING_CMD_ADD_BUFPOOL as u32,
        )
        .cmd(cmd)
        .buf_index(fixed.then_some(POOL_BUF_INDEX))
        .build()
    }

    /// CONSTELLATION PATCH (io-uring): the `READ_FIXED` of `read` into `e`'s slot. The
    /// destination address is an offset into the registered pages: a buffer the kernel
    /// registers itself (`io_buffer_register_bvec`) starts at 0.
    fn read_sqe(&self, e: &RingEntry, read: &ReadFixed) -> squeue::Entry128 {
        let fd = match &read.src {
            ReadSource::Caller(src) => src.as_fd().as_raw_fd(),
            ReadSource::Bounce => self.bounce_fd().unwrap_or(-1),
        };
        opcode::ReadFixed::new(types::Fd(fd), ptr::null_mut(), read.len, e.zc_slot)
            .offset(read.offset)
            .build()
            .user_data(READ_TAG | user_data(e.qid, e.idx))
            .into()
    }

    /// CONSTELLATION PATCH (io-uring): the queues of this ring and their entries' pools:
    /// whatever the session's handshake asked for (`reserve_pools`), what the kernel granted.
    /// In two rounds, each followed by `RingSet::start`'s barrier: this ring's buffer table
    /// (`set_up_table`), then -- only once every ring of the session has its table -- its
    /// queues and their pools (`set_up_queues`).
    ///
    /// Both run on the ring thread once the ring is enabled and before its first REGISTER --
    /// which is what makes a refusal here harmless. On 7.3 one failed REGISTER disables the
    /// ring for the whole connection, and a queue created for zero-copy refuses an entry with
    /// its own payload buffer; so nothing is registered until this has decided, for every
    /// queue, the one way its entries will be registered. The order puts the likeliest refusal
    /// first, while it still leaves nothing behind: a buffer table the process may not pin or
    /// account (`RLIMIT_MEMLOCK` without `CAP_IPC_LOCK`, `ENOMEM`), on any ring, then the first
    /// queue's `ADD_QUEUE` with `FUSE_URING_ZERO_COPY` (`EPERM` where the kernel's
    /// `capable(CAP_SYS_ADMIN)` says no, as in a user namespace whose `CapEff` shows it). Either
    /// one leaves every entry with its own payload buffer -- plain `Uring` -- and says why
    /// (`TableSetup::Refused`, `PoolSetup::Degraded`). A table refused on one ring withdraws
    /// every other ring's before any queue exists, so a session never ends up with zero-copy
    /// queues on some rings and not on others for that reason.
    fn set_up_table(&self, io: &mut RingIo) -> TableSetup {
        let mut plan = self.pool_plan.lock();
        let Some((mem, register)) = plan.as_ref() else {
            return TableSetup::NotAsked;
        };
        let slots = self.entries.len() + 1;
        let refused = if slots > IORING_MAX_REG_BUFFERS || self.entries.is_empty() {
            Some(format!(
                "ring {} has {} entries, more than the {} slots of an io_uring buffer table",
                self.index,
                self.entries.len(),
                IORING_MAX_REG_BUFFERS - 1
            ))
        } else {
            io.register_buffer_table(register.then(|| mem.iovec()), slots as u32)
                .err()
                .map(|err| {
                    format!(
                        "registering the buffer table of ring {} ({} bytes of pool, {slots} \
                         slots) failed ({err})",
                        self.index,
                        mem.len()
                    )
                })
        };
        match refused {
            Some(why) => {
                plan.take();
                TableSetup::Refused(why)
            }
            None => TableSetup::Ready,
        }
    }

    /// CONSTELLATION PATCH (io-uring): the second round of `set_up_table`: with `go` (every
    /// ring's table is registered) the queues and their pools, else this ring's table is
    /// released again and its entries keep their own payload buffers.
    ///
    /// Past the first `ADD_QUEUE` that queue exists as a zero-copy queue, which only ever
    /// accepts entries without a payload buffer, so the ring stays on pools: a later queue the
    /// kernel will not create for zero-copy is created without it (and the session is not
    /// zero-copy then), a pool it will not take as a fixed buffer is given unregistered, and
    /// only a queue or pool refused outright fails the ring -- `RegistrationRefused`, the
    /// session's constructor error, as for a refused REGISTER.
    fn set_up_queues(&self, io: &mut RingIo, go: bool) -> io::Result<PoolSetup> {
        let Some((mem, register)) = self.pool_plan.lock().take() else {
            return Ok(PoolSetup::NotAsked);
        };
        let release = |io: &mut RingIo| {
            if let Err(err) = io.unregister_buffer_table() {
                warn!(
                    "io_uring: ring {}: releasing the buffer table failed ({err})",
                    self.index
                );
            }
        };
        if !go {
            release(io);
            return Ok(PoolSetup::Degraded(format!(
                "ring {} withdrew its buffer table: another ring could not register one",
                self.index
            )));
        }
        let depth = self.depth.max(1) as usize;
        let qids: Vec<u16> = self.entries.iter().step_by(depth).map(|e| e.qid).collect();
        if let Err(err) = io.setup_cmd(self.add_queue_sqe(qids[0], true)) {
            release(io);
            return Ok(PoolSetup::Degraded(format!(
                "the kernel refused zero-copy queue {} ({err})",
                qids[0]
            )));
        }
        let mut queues = vec![
            QueuePool {
                zero_copy: true,
                fixed: false,
            };
            qids.len()
        ];
        let mut note = None;
        let refused = |qid: u16, err: io::Error| {
            crate::uring::RegistrationRefused::error(self.index, qid, err)
        };
        for (q, &qid) in qids.iter().enumerate().skip(1) {
            if let Err(err) = io.setup_cmd(self.add_queue_sqe(qid, true)) {
                note.get_or_insert(format!(
                    "the kernel created queue {qid} only without zero-copy ({err})"
                ));
                io.setup_cmd(self.add_queue_sqe(qid, false))
                    .map_err(|err| refused(qid, err))?;
                queues[q].zero_copy = false;
            }
        }
        for (q, &qid) in qids.iter().enumerate() {
            let (start, len) = mem.slice(q);
            if register {
                match io.setup_cmd(self.add_bufpool_sqe(qid, start, len, true)) {
                    Ok(()) => {
                        queues[q].fixed = true;
                        continue;
                    }
                    Err(err) => {
                        note.get_or_insert(format!(
                            "the kernel took queue {qid}'s pool only unregistered ({err})"
                        ));
                    }
                }
            }
            io.setup_cmd(self.add_bufpool_sqe(qid, start, len, false))
                .map_err(|err| refused(qid, err))?;
        }
        let zero_copy = queues.iter().all(|q| q.zero_copy);
        let _ = self.pools.set(Pools {
            mem: ManuallyDrop::new(mem),
            queues,
        });
        Ok(PoolSetup::Pools { zero_copy, note })
    }

    fn wake_sqe(&self) -> squeue::Entry128 {
        opcode::PollAdd::new(types::Fd(self.wake.as_raw_fd()), libc::POLLIN as u32)
            .multi(true)
            .build()
            .user_data(WAKE)
            .into()
    }

    /// Body of `fuser-ring-{r}`. `go` is released once the INIT reply was written,
    /// `registered` reports the REGISTER submit result back, `handler_rx` delivers the
    /// handler once the session runs.
    pub(crate) fn thread_main(
        self: Arc<Ring>,
        mut io: RingIo,
        go: mpsc::Receiver<()>,
        registered: mpsc::Sender<io::Result<()>>,
        handler_rx: mpsc::Receiver<Box<dyn FetchHandler>>,
    ) -> io::Result<()> {
        self.ring_thread.set(thread::current().id()).ok();
        if go.recv().is_err() {
            // Nothing was registered, so `in_kernel` is 0 and `Drop` unmaps
            return Ok(());
        }
        let reg = match (io.enable(), self.setup_gate.lock().take()) {
            (Err(err), None) => Err(err),
            (Ok(()), None) => self.register_all(&mut io),
            // CONSTELLATION PATCH (io-uring): the buffer table, then the queues and pools,
            // each followed by `RingSet::start`'s answer, which comes once every ring reported;
            // without one (a ring of the set failed) nothing is registered and the session
            // fails
            (enabled, Some(gate)) => {
                let table = enabled.map(|()| self.set_up_table(&mut io));
                let ok = table.is_ok();
                let _ = gate.table.send(table);
                let go = match gate.queues.recv() {
                    Ok(go) if ok => go,
                    _ => return Ok(()),
                };
                let setup = self.set_up_queues(&mut io, go);
                let ok = setup.is_ok();
                let _ = gate.setup.send(setup);
                if !ok || gate.proceed.recv().is_err() {
                    return Ok(());
                }
                self.register_all(&mut io)
            }
        };
        let ok = reg.is_ok();
        let _ = registered.send(reg);
        if !ok {
            // The session fails and its unmount completes whatever earlier batches submitted
            return Ok(());
        }
        debug!(
            "io_uring: ring {} registered {} entries",
            self.index,
            self.entries.len()
        );
        // Early fetches are deferred task work until the first io_uring_enter in `serve`
        let mut handler: Box<dyn FetchHandler> = match handler_rx.recv() {
            Ok(h) => h,
            Err(_) if self.live.lock().abandon => {
                debug!(
                    "io_uring: ring {} abandoning {} registered commands; the session failed \
                     to start",
                    self.index,
                    self.live.lock().in_kernel
                );
                return Ok(());
            }
            Err(_) if self.mounted => {
                error!(
                    "io_uring: ring {} serving EIO until the connection ends; the session was \
                     dropped before it was run",
                    self.index
                );
                Box::new(|c: RingCommit, _: &[u8]| c.commit_errno(Errno::EIO))
            }
            Err(_) => {
                // Returning closes the ring fd, which cancels the commands and releases their
                // /dev/fuse references so the connection aborts as it does without a ring
                error!(
                    "io_uring: ring {} abandoning {} registered commands; the from_fd session \
                     was dropped before it was run",
                    self.index,
                    self.live.lock().in_kernel
                );
                return Ok(());
            }
        };
        debug!("io_uring: ring {} serving", self.index);
        let outcome = self.serve(&mut io, &mut *handler);
        let mut live = self.live.lock();
        if outcome.is_err() {
            live.exited = true;
        }
        debug!(
            "io_uring: ring {} exited, in_kernel={} outstanding={}",
            self.index, live.in_kernel, live.outstanding
        );
        match outcome {
            Err(e) => Err(e),
            Ok(()) => live.fatal.take().map_or(Ok(()), Err),
        }
    }

    /// Pushes one REGISTER per entry plus the eventfd poll and submits once.
    ///
    /// CONSTELLATION PATCH (io-uring): the kernel validates a REGISTER when it is issued, inside
    /// that very `io_uring_enter`, and posts a refusal's CQE before the call returns; an
    /// accepted one completes only with a fetch. So the CQEs present right after the submit are
    /// the refusals, and one is an error here -- `RegistrationRefused`, which the session
    /// returns from its constructor -- rather than a fatal CQE in `serve` once the session
    /// already runs, with every request on the mount blocked on queues that never become ready.
    fn register_all(&self, io: &mut RingIo) -> io::Result<()> {
        self.live.lock().in_kernel = self.entries.len();
        for e in &self.entries {
            *e.state.lock() = EntryState::InKernel { last: 0 };
            io.push_or_submit(&self.register_sqe(e))?;
        }
        self.arm_wake(io)?;
        io.submit()?;
        let early = io.reap();
        #[cfg(test)]
        let early_check = !io.hooks.refusals_in_serve;
        #[cfg(not(test))]
        let early_check = true;
        let refused = early
            .iter()
            .find(|&&(ud, res, _)| early_check && ud != WAKE && res < 0);
        if let Some(&(ud, res, _)) = refused {
            // The refused entries never reached the kernel; whichever were accepted are
            // cancelled when this ring's io_uring is closed, and keep the mapping alive till then
            for &(ud, res, _) in &early {
                let (qid, idx) = decode(ud);
                let e = self.entries.get(idx as usize).filter(|e| e.qid == qid);
                if let (true, Some(e)) = (ud != WAKE && res < 0, e) {
                    self.retire(e, None);
                }
            }
            let (qid, _) = decode(ud);
            return Err(crate::uring::RegistrationRefused::error(
                self.index,
                qid,
                io::Error::from_raw_os_error(-res),
            ));
        }
        io.early.extend(early);
        Ok(())
    }

    fn arm_wake(&self, io: &mut RingIo) -> io::Result<()> {
        io.push_or_submit(&self.wake_sqe())
    }

    fn drain_eventfd(&self) {
        match self.wake.read() {
            Ok(_) | Err(nix::errno::Errno::EAGAIN) => {}
            Err(err) => debug!("io_uring: ring {} eventfd read failed: {err}", self.index),
        }
    }

    /// Pushes the COMMIT_AND_FETCH of every `Pending` entry. `Err` only for a ring-level
    /// failure; a per-entry failure retires the entry and continues.
    fn flush_pending(&self, io: &mut RingIo) -> io::Result<()> {
        #[cfg(test)]
        self.hooks.inject(io)?;
        let pending = std::mem::take(&mut self.live.lock().pending);
        for idx in pending {
            let e = &self.entries[idx as usize];
            let (sqe, commit_id) = {
                let mut state = e.state.lock();
                match std::mem::replace(&mut *state, EntryState::Dead) {
                    EntryState::Pending { commit_id } => {
                        *state = EntryState::InKernel { last: commit_id };
                        (self.commit_sqe(e, commit_id), commit_id)
                    }
                    // CONSTELLATION PATCH (io-uring): the `READ_FIXED` goes first; its
                    // completion pushes the commit (`read_done`)
                    EntryState::PendingRead { commit_id, read } => {
                        let sqe = self.read_sqe(e, &read);
                        *state = EntryState::Reading { commit_id, read };
                        (sqe, commit_id)
                    }
                    other => panic!(
                        "io_uring: entry {idx} queued in pending is {other:?}; every queued index \
                         is Pending (the ring thread stops; the mapping is leaked if any command \
                         is still counted)"
                    ),
                }
            };
            if let Err(err) = io.push_or_submit(&sqe) {
                if is_ring_failure(&err) {
                    return Err(err);
                }
                error!(
                    "io_uring: could not submit commit for unique {commit_id} on qid {}: {err}; \
                     the kernel request will not complete",
                    e.qid
                );
                self.retire(e, Some(err));
                continue;
            }
            #[cfg(test)]
            self.hooks.flushed(idx);
        }
        Ok(())
    }

    /// The command an `InKernel` entry has pending: `Some(0)` for REGISTER, else the
    /// commit id; `None` when the entry is not in the kernel, so its CQE is not ours to count.
    fn last_command(e: &RingEntry) -> Option<u64> {
        match *e.state.lock() {
            EntryState::InKernel { last } => Some(last),
            ref other => {
                error!(
                    "io_uring: CQE for qid {} entry {} which is {other:?}, not in the kernel",
                    e.qid, e.idx
                );
                None
            }
        }
    }

    /// Re-pushes the last command of an entry after `-EAGAIN` or `-EINTR`.
    fn resubmit(&self, io: &mut RingIo, e: &RingEntry, last: u64) -> io::Result<()> {
        #[cfg(test)]
        self.hooks.resubmitted.lock().push((e.idx, last));
        let sqe = match last {
            0 => self.register_sqe(e),
            commit_id => self.commit_sqe(e, commit_id),
        };
        io.push_or_submit(&sqe)
    }

    /// The entry leaves the kernel for good.
    fn retire(&self, e: &RingEntry, err: Option<io::Error>) {
        let mut state = e.state.lock();
        let mut live = self.live.lock();
        live.kill(e, &mut state);
        dec(&mut live.in_kernel, e, "in-kernel");
        if let Some(err) = err {
            live.fatal.get_or_insert(err);
        }
    }

    /// The buffers are written; queues the entry for the ring thread.
    fn hand_off(&self, e: &RingEntry, commit_id: u64) {
        self.hand_off_as(e, commit_id, EntryState::Pending { commit_id });
    }

    /// CONSTELLATION PATCH (io-uring): queues a `READ_FIXED` reply for the ring thread,
    /// which commits once the read completed (`read_done`).
    fn hand_off_read(&self, e: &RingEntry, commit_id: u64, read: ReadFixed) {
        self.hand_off_as(e, commit_id, EntryState::PendingRead { commit_id, read });
    }

    /// CONSTELLATION PATCH (io-uring): called at the top of the pass after an idle wait timed
    /// out with no completion at all. A commit another thread queued in `pending` more than
    /// `STRANDED_AFTER_MS` before that is one whose eventfd announcement never produced a
    /// completion: without the bounded wait the kernel's caller would wait for it until some
    /// other request on this ring's queues woke the thread. It is flushed by this very pass;
    /// here it is counted (`RingHealth::stranded_commits`) and reported, at most once a minute
    /// per ring.
    fn note_stranded(&self) {
        let now = mono_ms();
        let (stranded, oldest) = {
            let live = self.live.lock();
            live.pending
                .iter()
                .map(|&idx| self.entries[idx as usize].handed_ms.load(Ordering::Relaxed))
                .filter(|&at| at != 0 && now.saturating_sub(at) >= STRANDED_AFTER_MS)
                .fold((0u64, 0u64), |(n, oldest), at| (n + 1, oldest.max(now - at)))
        };
        if stranded == 0 {
            return;
        }
        let total = self
            .health
            .get()
            .map_or(stranded, |h| h.note_stranded(stranded));
        let last = self.stranded_logged.load(Ordering::Relaxed);
        if last == 0 || now.saturating_sub(last) >= 60_000 {
            self.stranded_logged.store(now, Ordering::Relaxed);
            warn!(
                "io_uring: ring {} found {stranded} repl{} queued with no wake-up (oldest \
                 {oldest} ms); flushed by the idle wait ({total} so far in this session)",
                self.index,
                if stranded == 1 { "y" } else { "ies" }
            );
        }
    }

    /// CONSTELLATION PATCH (io-uring): the entries of this ring that userspace has held for at
    /// least `after_ms`: fetched from the kernel, not yet committed back. Each one is a
    /// caller waiting in the kernel on a request the filesystem may not even know about yet
    /// (still queued for an offload thread), or whose reply is not yet on its way back. Every
    /// newly long-held entry is logged once per fetch with its state, opcode and age, next to
    /// a per-queue census; blocking lock requests (which wait for the lock holder by design)
    /// are logged at debug level and left out of the returned count.
    pub(crate) fn report_held(&self, after_ms: u64) -> u64 {
        let now = mono_ms();
        let mut long = 0;
        let mut new = Vec::new();
        let mut census: std::collections::BTreeMap<u16, [u32; 3]> = Default::default();
        for e in self.entries.iter() {
            let state = e.state.lock();
            let held = !matches!(*state, EntryState::InKernel { .. } | EntryState::Dead);
            // [in the kernel, held by userspace, dead]
            let slot = match *state {
                EntryState::InKernel { .. } => 0,
                EntryState::Dead => 2,
                _ => 1,
            };
            census.entry(e.qid).or_default()[slot] += 1;
            let at = e.fetched_ms.load(Ordering::Relaxed);
            if !held || at == 0 || now.saturating_sub(at) < after_ms {
                continue;
            }
            let lock_wait = e.lock_wait.load(Ordering::Relaxed);
            if !lock_wait {
                long += 1;
            }
            let unique = e.fetched_unique.load(Ordering::Relaxed);
            if e.held_reported.swap(unique, Ordering::Relaxed) != unique {
                new.push((
                    lock_wait,
                    format!(
                        "qid {} entry {} unique {unique} opcode {} {:?} for {} ms",
                        e.qid,
                        e.idx,
                        e.fetched_opcode.load(Ordering::Relaxed),
                        *state,
                        now - at
                    ),
                ));
            }
        }
        if new.is_empty() {
            return long;
        }
        let census: Vec<String> = census
            .iter()
            .map(|(qid, [k, u, d])| format!("{qid}: {k}/{u}/{d}"))
            .collect();
        let (pending, outstanding) = {
            let live = self.live.lock();
            (live.pending.len(), live.outstanding)
        };
        for (lock_wait, entry) in new {
            if lock_wait {
                debug!(
                    "io_uring: ring {} holds a blocking lock request: {entry}",
                    self.index
                );
            } else {
                warn!(
                    "io_uring: ring {} holds a request in userspace past {after_ms} ms: {entry}; \
                     outstanding {outstanding}, pending {pending}; per queue in kernel/userspace/\
                     dead: {}",
                    self.index,
                    census.join(", ")
                );
            }
        }
        long
    }

    /// CONSTELLATION PATCH (io-uring): writes a reply (`iov[0]` the out header) the way the
    /// current fetch takes it, and hands the entry off. A zero-copied request's payload cannot
    /// go through the payload buffer -- the kernel copies nothing from it into the request's
    /// pages -- so a reply with data is bounced into them with a `READ_FIXED`
    /// (`bounce_in`); anything else is `write_reply`'s. The caller made the entry
    /// `Committing`, and no request slice is live.
    fn deliver(&self, e: &RingEntry, commit_id: u64, iov: &[IoSlice<'_>]) {
        let payload = iov.get(1..).unwrap_or_default();
        if e.zero_copied.load(Ordering::Relaxed) && payload.iter().any(|s| !s.is_empty()) {
            match self.bounce_in(e, payload) {
                Ok(read) => return self.hand_off_read(e, commit_id, read),
                Err(err) => {
                    error!(
                        "io_uring: bouncing the reply to zero-copied unique {commit_id} failed \
                         ({err}); replying EIO"
                    );
                    // SAFETY: the caller's guarantee.
                    unsafe { e.write_errno(commit_id, Errno::EIO) };
                    return self.hand_off(e, commit_id);
                }
            }
        }
        // SAFETY: the caller's guarantee.
        unsafe { e.write_reply(commit_id, iov) };
        self.hand_off(e, commit_id);
    }

    /// CONSTELLATION PATCH (io-uring): the bounce memfd's descriptor, creating it on first use.
    fn bounce_fd(&self) -> Result<std::os::fd::RawFd, String> {
        self.bounce
            .get_or_init(|| {
                // SAFETY: a plain syscall with a static, NUL-terminated name.
                let fd = unsafe {
                    libc::memfd_create(c"fuser-zero-copy-bounce".as_ptr(), libc::MFD_CLOEXEC)
                };
                if fd < 0 {
                    return Err(format!(
                        "memfd_create failed ({})",
                        io::Error::last_os_error()
                    ));
                }
                debug!(
                    "io_uring: ring {} bounces replies to zero-copied requests given as bytes \
                     through a memfd",
                    self.index
                );
                // SAFETY: `fd` was just created and is owned by nothing else.
                Ok(File::from(unsafe { OwnedFd::from_raw_fd(fd) }))
            })
            .as_ref()
            .map(|f| f.as_raw_fd())
            .map_err(Clone::clone)
    }

    /// CONSTELLATION PATCH (io-uring): writes `payload` into the bounce memfd at entry `e`'s
    /// own range and returns the `READ_FIXED` that carries it into the request's pages: two
    /// copies (into the memfd's pages, out of them), against the one a non-zero-copy queue
    /// makes -- the price of answering a zero-copy open from memory rather than a file, which
    /// the read path of plan 38 §3(d) leaves to reads that span chunks. The range is punched
    /// out again once the read completed (`read_done`), so the memfd holds only replies in
    /// flight.
    fn bounce_in(&self, e: &RingEntry, payload: &[IoSlice<'_>]) -> io::Result<ReadFixed> {
        let fd = self.bounce_fd().map_err(io::Error::other)?;
        // SAFETY: `bounce` owns the descriptor for the ring's lifetime.
        let file = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
        let total: usize = payload.iter().map(|s| s.len()).sum();
        let len = u32::try_from(total)
            .ok()
            .filter(|_| total <= e.reply_cap().max(e.payload_cap))
            .ok_or_else(|| io::Error::other(format!("a reply of {total} bytes")))?;
        let at = u64::from(e.idx) * e.payload_cap as u64;
        let mut written = 0usize;
        let mut slices: SmallVec<[IoSlice<'_>; 4]> = payload.iter().copied().collect();
        let mut rest: &mut [IoSlice<'_>] = &mut slices;
        while written < total {
            let n = nix::sys::uio::pwritev(file, &*rest, (at + written as u64) as libc::off_t)?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            written += n;
            IoSlice::advance_slices(&mut rest, n);
        }
        Ok(ReadFixed {
            src: ReadSource::Bounce,
            offset: at,
            len,
        })
    }

    /// CONSTELLATION PATCH (io-uring): the completion of entry `ud`'s `READ_FIXED`: its result
    /// becomes the reply -- `res` bytes, now in the request's pages, or `-res` as the error --
    /// and the entry's COMMIT_AND_FETCH is pushed. A short read of a bounced reply is `EIO`:
    /// the memfd held all of it. `Err` only for a ring-level failure.
    fn read_done(&self, io: &mut RingIo, ud: u64, res: i32) -> io::Result<()> {
        let (qid, idx) = decode(ud);
        let Some(e) = self.entries.get(idx as usize).filter(|e| e.qid == qid) else {
            error!("io_uring: READ_FIXED CQE for unknown entry qid={qid} idx={idx}");
            return Ok(());
        };
        let mut state = e.state.lock();
        let commit_id = match &*state {
            EntryState::Reading { commit_id, .. } => *commit_id,
            other => {
                error!(
                    "io_uring: READ_FIXED CQE for qid {qid} entry {idx} which is {other:?}, not \
                     reading"
                );
                return Ok(());
            }
        };
        if res == -libc::EAGAIN || res == -libc::EINTR {
            let EntryState::Reading { read, .. } = &*state else {
                unreachable!("matched just above");
            };
            let sqe = self.read_sqe(e, read);
            drop(state);
            return match io.push_or_submit(&sqe) {
                Err(err) if is_ring_failure(&err) => Err(err),
                Err(err) => {
                    self.retire(e, Some(err));
                    Ok(())
                }
                Ok(()) => Ok(()),
            };
        }
        let EntryState::Reading { read, .. } =
            std::mem::replace(&mut *state, EntryState::InKernel { last: commit_id })
        else {
            unreachable!("matched just above");
        };
        drop(state);
        if let ReadSource::Bounce = read.src {
            if let Ok(fd) = self.bounce_fd() {
                // SAFETY: `bounce` owns the descriptor for the ring's lifetime.
                let file = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
                let punched = nix::fcntl::fallocate(
                    file,
                    nix::fcntl::FallocateFlags::FALLOC_FL_PUNCH_HOLE
                        | nix::fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE,
                    read.offset as libc::off_t,
                    libc::off_t::from(read.len),
                );
                if let Err(err) = punched {
                    debug!("io_uring: releasing a bounced reply failed ({err})");
                }
            }
        }
        let len = read.len;
        // A bounced reply is all in the memfd: fewer bytes than were written to it is a
        // failure, not the end of a file, and must not reach the reader as a short read
        let whole = matches!(read.src, ReadSource::Bounce);
        drop(read);
        // SAFETY: the entry is the ring thread's alone: its state names a command in the
        // kernel, so no committer writes it, and the kernel holds no command for it until the
        // commit below is pushed.
        unsafe {
            match u32::try_from(res) {
                Ok(n) if n == len || (n < len && !whole) => {
                    let header = abi::fuse_out_header {
                        len: OUT_HEADER_SZ as u32 + n,
                        error: 0,
                        unique: commit_id,
                    };
                    e.write_header(header.as_bytes(), n);
                }
                Ok(n) => {
                    error!(
                        "io_uring: READ_FIXED of {len} bytes{} returned {n}; replying EIO",
                        if whole { " of a bounced reply" } else { "" }
                    );
                    e.write_errno(commit_id, Errno::EIO);
                }
                Err(_) => e.write_errno(commit_id, Errno::from_i32(-res)),
            }
        }
        match io.push_or_submit(&self.commit_sqe(e, commit_id)) {
            Err(err) if is_ring_failure(&err) => Err(err),
            Err(err) => {
                error!(
                    "io_uring: could not submit commit for unique {commit_id} on qid {qid}: \
                     {err}; the kernel request will not complete"
                );
                self.retire(e, Some(err));
                Ok(())
            }
            Ok(()) => Ok(()),
        }
    }

    /// The buffers are written (or, CONSTELLATION PATCH (io-uring), a `READ_FIXED` is to
    /// write them); queues the entry for the ring thread in state `next`.
    fn hand_off_as(&self, e: &RingEntry, commit_id: u64, next: EntryState) {
        {
            let mut state = e.state.lock();
            let mut live = self.live.lock();
            if live.exited {
                live.kill(e, &mut state);
                drop(live);
                drop(state);
                debug!(
                    "io_uring: dropping reply for unique {commit_id} after ring {} exited",
                    self.index
                );
                return;
            }
            dec(&mut live.outstanding, e, "outstanding");
            live.release_lock_wait(e);
            *state = next;
            live.in_kernel += 1;
            live.pending.push(e.idx);
            e.handed_ms.store(mono_ms(), Ordering::Relaxed);
        }
        #[cfg(test)]
        if self
            .hooks
            .lost_wakes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return;
        }
        if self.ring_thread.get() != Some(&thread::current().id()) {
            if let Err(err) = self.wake.write(1) {
                // The commit stays queued; the next CQE-driven pass picks it up
                error!("io_uring: eventfd write failed: {err}");
            }
        }
    }

    /// Returns `Ok` on a clean drain with `live.exited` set, `Err` when the ring is unusable.
    ///
    /// The ring leaves once nothing is pending in the kernel and either the connection ended
    /// or the session shut down (requests still held by userspace are stranded by the kernel
    /// anyway), or a fatal error was recorded and no fetched request is still held, since
    /// leaving earlier would drop the replies of those requests and hang the applications
    /// behind them.
    fn serve(self: &Arc<Self>, io: &mut RingIo, handler: &mut dyn FetchHandler) -> io::Result<()> {
        let (mut wake_retried, mut wake_dead) = (false, false);
        // CONSTELLATION PATCH (io-uring): the last wait ended with no completion at all
        let mut timed_out = false;
        loop {
            if std::mem::take(&mut timed_out) {
                self.note_stranded();
            }
            self.flush_pending(io)?;
            {
                let mut live = self.live.lock();
                let drained = live.conn_dead
                    || live.shutdown
                    || (live.fatal.is_some() && live.outstanding == 0);
                if live.in_kernel == 0 && drained {
                    live.exited = true;
                    return Ok(());
                }
            }
            #[cfg(test)]
            self.hooks
                .exit_checks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Once the eventfd poll is dead foreign commits cannot wake this thread, so the
            // wait is short and flush_pending runs on every pass. CONSTELLATION PATCH
            // (io-uring): otherwise it is `IDLE_WAIT`, so that a commit whose wake-up was lost
            // is flushed (and reported) rather than stranded
            let wait = if wake_dead { DEAD_WAKE_WAIT } else { IDLE_WAIT };
            match io.submit_and_wait(wait) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) if e.raw_os_error() == Some(libc::ETIME) => {
                    timed_out = !wake_dead;
                    continue;
                }
                // The SQ was not consumed; reap what the CQ holds so it can be retried, and
                // give the kernel a moment since the CQ is usually empty on EAGAIN
                Err(e) if matches!(e.raw_os_error(), Some(libc::EBUSY | libc::EAGAIN)) => {
                    debug!("io_uring: ring {} enter failed ({e}); retrying", self.index);
                    thread::yield_now();
                }
                Err(e) => return Err(e),
            }
            let cqes = io.reap();
            // The in-memory backend reports a timed-out wait as one with no CQE
            timed_out = cqes.is_empty() && !wake_dead;
            for (ud, res, flags) in cqes {
                if ud == WAKE {
                    let failed = if res < 0 {
                        #[cfg(test)]
                        self.hooks
                            .poll_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let err = io::Error::from_raw_os_error(-res);
                        if !wake_retried && self.arm_wake(io).is_ok() {
                            wake_retried = true;
                            warn!(
                                "io_uring: ring {} eventfd poll failed ({err}); re-armed once",
                                self.index
                            );
                            None
                        } else {
                            Some(err)
                        }
                    } else {
                        self.drain_eventfd();
                        if cqueue::more(flags) {
                            None
                        } else {
                            // The multishot poll ended; a failed re-arm degrades like a
                            // failed poll rather than ending the ring
                            match self.arm_wake(io) {
                                Ok(()) => None,
                                Err(e) if is_ring_failure(&e) => return Err(e),
                                Err(e) => Some(e),
                            }
                        }
                    };
                    if let Some(err) = failed {
                        error!(
                            "io_uring: ring {} eventfd poll failed ({err}); polling pending \
                             commits every 10 ms until the ring exits",
                            self.index
                        );
                        self.live.lock().fatal.get_or_insert(err);
                        wake_dead = true;
                    }
                    continue;
                }
                // CONSTELLATION PATCH (io-uring): a `READ_FIXED` into a request's pages
                if ud & READ_TAG != 0 {
                    self.read_done(io, ud & !READ_TAG, res)?;
                    continue;
                }
                let (qid, idx) = decode(ud);
                let Some(e) = self.entries.get(idx as usize).filter(|e| e.qid == qid) else {
                    error!("io_uring: CQE for unknown entry qid={qid} idx={idx}");
                    #[cfg(test)]
                    self.hooks
                        .ignored
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                };
                let Some(last) = Self::last_command(e) else {
                    #[cfg(test)]
                    self.hooks
                        .ignored
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                };
                match res {
                    0 => self.handle_fetch(e, handler),
                    r if r == -libc::EAGAIN || r == -libc::EINTR => {
                        thread::yield_now();
                        if let Err(err) = self.resubmit(io, e, last) {
                            if is_ring_failure(&err) {
                                return Err(err);
                            }
                            self.retire(e, Some(err));
                        }
                    }
                    r if r == -libc::ENOTCONN || r == -libc::ECONNABORTED => {
                        self.live.lock().conn_dead = true;
                        self.retire(e, None);
                    }
                    r => self.fail_entry(e, r, last),
                }
            }
        }
    }

    /// A CQE that ends the entry: log what it means and retire it as fatal. `last` is the
    /// command that failed, 0 for REGISTER.
    fn fail_entry(&self, e: &RingEntry, res: i32, last: u64) {
        let err = if res > 0 {
            io::Error::other(format!("unexpected result {res}"))
        } else {
            io::Error::from_raw_os_error(-res)
        };
        match -res {
            libc::ENOENT => error!(
                "io_uring: commit for unknown commit_id {last} on qid {}; that queue has \
                 permanently lost one kernel entry",
                e.qid
            ),
            libc::ECANCELED => error!(
                "io_uring: qid {} entry {} cancelled; its submitting thread exited",
                e.qid, e.idx
            ),
            _ => error!("io_uring: qid {} entry {} failed: {err}", e.qid, e.idx),
        }
        let register_rejected =
            last == 0 && matches!(-res, libc::EINVAL | libc::EOPNOTSUPP | libc::EFAULT);
        if register_rejected && self.live.lock().fatal.is_none() {
            error!(
                "io_uring: the kernel rejected the registration of ring {} ({err}); the \
                 session ends once the ring's entries are back",
                self.index
            );
        }
        self.retire(e, Some(err));
    }

    /// A CQE with `res == 0`: stage, dispatch, and finish whatever dispatch left behind.
    fn handle_fetch(self: &Arc<Self>, e: &RingEntry, handler: &mut dyn FetchHandler) {
        // CONSTELLATION PATCH (io-uring): where this fetch's payload is, and whether it is
        // served at all; see `locate_payload`
        // SAFETY: the CQE for this entry just arrived, so the kernel is done writing the
        // header and the request's payload buffer, and no reference into either exists.
        let refusal = unsafe { self.locate_payload(e) };
        // SAFETY: the CQE for this entry just arrived, so the kernel is done writing the
        // stride and no reference into it exists.
        let staged = match unsafe { stage_request(e.base.0, e.gap, e.payload_cap) } {
            Ok(staged) => staged,
            Err(StagingError::ZeroCommitId) => {
                error!("io_uring: fetched entry with commit_id 0 on qid {}", e.qid);
                self.retire(e, Some(io::Error::other("fetched entry with commit_id 0")));
                return;
            }
            Err(StagingError::Malformed {
                commit_id,
                in_len,
                payload_sz,
            }) => {
                error!(
                    "io_uring: malformed fetch on qid {} (len {in_len}, payload_sz {payload_sz}); \
                     replying EIO",
                    e.qid
                );
                // SAFETY: as above, the ring thread is the entry's only writer right now.
                unsafe { e.write_errno(commit_id, Errno::EIO) };
                // The entry stays counted in `in_kernel`; it goes straight back
                *e.state.lock() = EntryState::Pending { commit_id };
                self.live.lock().pending.push(e.idx);
                return;
            }
        };
        let commit_id = staged.commit_id;
        {
            // CONSTELLATION PATCH (io-uring): what `report_held` names this fetch by
            // SAFETY: as for `stage_request` above; the slice ends with this block.
            let req = unsafe { slice::from_raw_parts(staged.req.as_ptr(), staged.len.min(8)) };
            let opcode = req
                .get(4..8)
                .map_or(0, |b| u32::from_ne_bytes(b.try_into().unwrap()));
            e.fetched_opcode.store(opcode, Ordering::Relaxed);
            e.fetched_unique.store(commit_id, Ordering::Relaxed);
            e.fetched_ms.store(mono_ms(), Ordering::Relaxed);
        }
        if let Some((kind, why)) = refusal {
            // Once per kind of refusal, so a second kind still shows up in the log
            static ONCE: [std::sync::Once; 3] = [const { std::sync::Once::new() }; 3];
            ONCE[kind as usize].call_once(|| error!("io_uring: {why}; replying EIO (logged once)"));
            // SAFETY: as above, the ring thread is the entry's only writer right now.
            unsafe { e.write_errno(commit_id, Errno::EIO) };
            *e.state.lock() = EntryState::Pending { commit_id };
            self.live.lock().pending.push(e.idx);
            return;
        }
        {
            let mut state = e.state.lock();
            *state = EntryState::Dispatching {
                direct_ok: staged.payload_sz == 0,
                reply_taken: false,
                commit_id,
            };
            let mut live = self.live.lock();
            dec(&mut live.in_kernel, e, "in-kernel");
            live.outstanding += 1;
        }
        e.held.store(false, std::sync::atomic::Ordering::Relaxed);
        e.lock_downgraded
            .store(false, std::sync::atomic::Ordering::Relaxed);
        {
            // SAFETY: `stage_request` made `[req, req + len)` one contiguous request inside
            // the stride; the slice ends with this block, before the entry is touched again
            // (a handler that kept it did so through `RingCommit::hold`, whose `HeldRequest`
            // ends the dispatch itself).
            let request = unsafe { slice::from_raw_parts(staged.req.as_ptr(), staged.len) };
            let commit = RingCommit {
                ring: Arc::clone(self),
                idx: e.idx,
                commit_id,
            };
            handler.handle(commit, request);
        }
        if e.held.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        self.finish_dispatch(e, commit_id, false);
    }

    /// CONSTELLATION PATCH (io-uring): records where the reply of the fetch that just arrived
    /// in `e` goes and whether the request was zero-copied, and on a queue with a pool copies
    /// the request's payload from the pool buffer the kernel picked to the entry's own payload
    /// buffer, where `stage_request` expects it to continue the staged header (a `FUSE_WRITE`'s
    /// data is then one copy away from the kernel's, where without a pool it is none).
    /// `Some(why)` refuses the request with `EIO`: a pool offset the kernel never hands out, a
    /// payload larger than a pool buffer, or a zero-copied request that is not a read -- a
    /// zero-copied write's data is only in the registered pages, which nothing here reads
    /// (`ReplyOpen::opened_zero_copy` is for read-only opens).
    ///
    /// # Safety
    ///
    /// The entry's fetch CQE just arrived and no reference into its buffers exists.
    ///
    /// A request the kernel gives no pool buffer (nothing to copy either way: `FLUSH`,
    /// `RELEASE`, `FSYNC`, ...) arrives at offset 0, indistinguishable from the pool's first
    /// buffer, which another entry may hold. Its `reply_cap` is 0 (`has_out_args`), so a reply
    /// with a payload becomes `EINVAL` in `write_reply` rather than landing in that buffer.
    unsafe fn locate_payload(&self, e: &RingEntry) -> Option<(Refusal, String)> {
        let own = e.own_payload();
        let Some(pools) = self.pools.get() else {
            e.reply_at.store(own, Ordering::Relaxed);
            e.reply_cap.store(e.payload_cap, Ordering::Relaxed);
            e.zero_copied.store(false, Ordering::Relaxed);
            return None;
        };
        let base = e.base.0.as_ptr();
        // SAFETY: the trailer fields lie inside the entry's header area; the reads are
        // unaligned so no reference is formed over kernel-shared memory.
        let (flags, offset, payload_sz, opcode) = unsafe {
            (
                ptr::read_unaligned(base.add(FLAGS_OFFSET).cast::<u64>()),
                ptr::read_unaligned(base.add(POOL_OFFSET_OFFSET).cast::<u32>()),
                ptr::read_unaligned(base.add(PAYLOAD_SZ_OFFSET).cast::<u32>()),
                ptr::read_unaligned(base.add(4).cast::<u32>()),
            )
        };
        let queue = pools.queues.get(e.queue as usize).copied();
        let zero_copied =
            queue.is_some_and(|q| q.zero_copy) && flags & abi::FUSE_URING_ENT_ZERO_COPY != 0;
        let buf_size = pools.mem.buf_size();
        e.zero_copied.store(zero_copied, Ordering::Relaxed);
        e.reply_at.store(own, Ordering::Relaxed);
        let reply_cap = if has_out_args(opcode) {
            buf_size.min(e.payload_cap)
        } else {
            0
        };
        e.reply_cap.store(reply_cap, Ordering::Relaxed);
        let Some(buf) = pools.mem.buffer(e.queue as usize, offset) else {
            return Some((
                Refusal::Offset,
                format!(
                    "the kernel put a request of qid {} at offset {offset} of its pool, which \
                     is not one of its buffers",
                    e.qid
                ),
            ));
        };
        e.reply_at.store(buf.as_ptr(), Ordering::Relaxed);
        if zero_copied {
            return (opcode != abi::fuse_opcode::FUSE_READ as u32).then(|| {
                (
                    Refusal::ZeroCopied,
                    format!(
                        "the kernel zero-copied a request with opcode {opcode} on qid {}, \
                         which this crate does not serve: only reads of a \
                         FOPEN_IO_URING_ZERO_COPY open can be answered from the request's pages",
                        e.qid
                    ),
                )
            });
        }
        let len = payload_sz as usize;
        if len > buf_size.min(e.payload_cap) {
            return Some((
                Refusal::Oversize,
                format!(
                    "a request of qid {} claims a {len} byte payload, more than a {buf_size} \
                     byte pool buffer",
                    e.qid
                ),
            ));
        }
        // SAFETY: `len` bytes lie inside both the pool buffer and the entry's own payload
        // buffer, which are separate mappings; the caller's guarantee covers both.
        unsafe { ptr::copy_nonoverlapping(buf.as_ptr(), own, len) };
        None
    }

    /// The end of a dispatch: a reply stashed during it is written now, a request the
    /// filesystem was given no reply object for is answered with an empty one, and otherwise
    /// the entry waits for its reply as `Dispatched`. Run by the ring thread right after its
    /// handler returns, or -- CONSTELLATION PATCH (io-uring) -- by a `HeldRequest`'s drop on
    /// the thread that took the dispatch over (`held`), where the ring may have exited
    /// meanwhile and left the entry `Dead`.
    fn finish_dispatch(&self, e: &RingEntry, commit_id: u64, held: bool) {
        let reply: Option<(Vec<u8>, u64)> = {
            let mut state = e.state.lock();
            match &mut *state {
                EntryState::Deferred {
                    reply: Stashed::Bytes(bytes),
                    commit_id,
                } => {
                    let reply = (std::mem::take(&mut bytes.0), *commit_id);
                    *state = EntryState::Committing;
                    Some(reply)
                }
                // CONSTELLATION PATCH (io-uring): a `read_fixed` made while the request was
                // held goes to the ring thread now
                EntryState::Deferred {
                    reply: Stashed::Read(_),
                    commit_id,
                } => {
                    let commit_id = *commit_id;
                    let EntryState::Deferred {
                        reply: Stashed::Read(read),
                        ..
                    } = std::mem::replace(&mut *state, EntryState::Committing)
                    else {
                        unreachable!("matched just above");
                    };
                    drop(state);
                    self.hand_off_read(e, commit_id, read);
                    return;
                }
                EntryState::Dispatching {
                    reply_taken: false, ..
                } => {
                    *state = EntryState::Committing;
                    let header = abi::fuse_out_header {
                        len: OUT_HEADER_SZ as u32,
                        error: 0,
                        unique: commit_id,
                    };
                    Some((header.as_bytes().to_vec(), commit_id))
                }
                EntryState::Dispatching {
                    reply_taken: true, ..
                } => {
                    *state = EntryState::Dispatched { commit_id };
                    None
                }
                // A reply already happened, or another thread is writing one right now
                EntryState::Pending { .. }
                | EntryState::PendingRead { .. }
                | EntryState::Reading { .. }
                | EntryState::Committing => None,
                // The ring exited while a held dispatch ran; a reply would have nowhere to go
                EntryState::Dead if held => None,
                other @ (EntryState::InKernel { .. }
                | EntryState::Dispatched { .. }
                | EntryState::Dead) => panic!(
                    "io_uring: entry {} is {other:?} right after dispatch; only the ring thread \
                     pushes SQEs, marks entries dispatched and marks them dead (the ring thread \
                     stops; the mapping is leaked if any command is still counted)",
                    e.idx
                ),
            }
        };
        if let Some((bytes, commit_id)) = reply {
            let (header, payload) = bytes.split_at(bytes.len().min(OUT_HEADER_SZ));
            // `Committing`, and the request slice is gone
            self.deliver(e, commit_id, &[IoSlice::new(header), IoSlice::new(payload)]);
        }
    }
}

impl Drop for Ring {
    /// Unmapping memory the kernel may still write into would corrupt whatever the allocator
    /// places there, so the mapping is only released once every command has completed.
    fn drop(&mut self) {
        let in_kernel = self.live.get_mut().in_kernel;
        if in_kernel == 0 {
            // SAFETY: dropped exactly once, here, and never used afterwards.
            unsafe { ManuallyDrop::drop(&mut self.mem) };
            // CONSTELLATION PATCH (io-uring): the pools, likewise
            if let Some(pools) = self.pools.get_mut() {
                // SAFETY: as above.
                unsafe { ManuallyDrop::drop(&mut pools.mem) };
            }
        } else {
            error!(
                "io_uring: ring {} leaking {} bytes of ring buffers because {in_kernel} \
                 commands are still pending in the kernel",
                self.index,
                self.mem.len()
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs::File;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc::Sender;
    use std::time::Duration;
    use std::time::Instant;

    use super::*;
    use crate::ll::AnyRequest;
    use crate::ll::Operation;
    use crate::ll::fuse_abi::fuse_opcode;
    use crate::uring::mem::COMMIT_ID_OFFSET;
    use crate::uring::mem::OP_IN_OFFSET;
    use crate::uring::mem::test::UNMAP_CHECK;
    use crate::uring::mem::test::is_mapped;
    use crate::uring::staging::test::in_header;

    /// Test-only observation and fault injection on the io_uring.
    #[derive(Default)]
    pub(super) struct IoHooks {
        /// Errno the next `submit` fails with instead of entering the kernel.
        fail_submit: Option<i32>,
        submits: usize,
        /// Errno `enable` fails with instead of enabling the ring.
        fail_enable: Option<i32>,
        /// CONSTELLATION PATCH (io-uring): leave the REGISTER refusals `register_all` sees at
        /// once to `serve`, as before it checked for them -- for the tests that use a non-FUSE
        /// device's synchronous `EOPNOTSUPP` to exercise `serve`'s handling of a fatal CQE.
        pub(super) refusals_in_serve: bool,
    }

    impl IoHooks {
        pub(super) fn before_submit(&mut self) -> io::Result<()> {
            self.submits += 1;
            match self.fail_submit.take() {
                Some(errno) => Err(io::Error::from_raw_os_error(errno)),
                None => Ok(()),
            }
        }

        pub(super) fn before_enable(&mut self) -> io::Result<()> {
            self.fail_enable
                .take()
                .map_or(Ok(()), |errno| Err(io::Error::from_raw_os_error(errno)))
        }
    }

    /// Test-only hooks into the ring thread's loop.
    #[derive(Default)]
    pub(super) struct RingHooks {
        /// Receives the index of every entry whose COMMIT_AND_FETCH `flush_pending` pushed.
        flushed: Mutex<Option<Sender<u32>>>,
        /// SQEs the ring thread pushes at the top of its next pass.
        inject: Mutex<Vec<squeue::Entry128>>,
        /// `WAKE` CQEs with a negative result seen so far.
        pub(super) poll_failed: AtomicUsize,
        /// `(idx, last command)` of every `resubmit`.
        pub(super) resubmitted: Mutex<Vec<(u32, u64)>>,
        /// CQEs dropped because they named no counted command.
        pub(super) ignored: AtomicUsize,
        /// Passes of `serve` that evaluated the exit test and stayed.
        pub(super) exit_checks: AtomicUsize,
        /// `fill` replies written straight into an entry, as opposed to handed back.
        pub(super) direct_fills: AtomicUsize,
        /// CONSTELLATION PATCH (io-uring): this many next hand-offs queue their commit without
        /// writing the wake eventfd -- a lost announcement.
        pub(super) lost_wakes: AtomicUsize,
    }

    impl RingHooks {
        pub(super) fn flushed(&self, idx: u32) {
            if let Some(tx) = &*self.flushed.lock() {
                tx.send(idx).unwrap();
            }
        }

        pub(super) fn inject(&self, io: &mut RingIo) -> io::Result<()> {
            for sqe in std::mem::take(&mut *self.inject.lock()) {
                io.push_or_submit(&sqe)?;
            }
            Ok(())
        }
    }

    fn sqe_from_bytes(raw: [u8; 128]) -> squeue::Entry128 {
        // SAFETY: as in `set_sqe_len`.
        unsafe { std::mem::transmute(raw) }
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

    /// A device without a `uring_cmd` operation, so every command fails with `-EOPNOTSUPP`.
    /// Not `/dev/null`: since Linux 7.0 it answers `uring_cmd` with 0
    const NOT_FUSE: &str = "/dev/zero";

    fn not_fuse() -> Arc<DevFuse> {
        Arc::new(DevFuse(File::open(NOT_FUSE).unwrap()))
    }

    /// A ring of `n` entries on a non-FUSE device, one queue per entry
    fn fake_ring(n: u16, mounted: bool) -> Arc<Ring> {
        let qids: Vec<u16> = (0..n).collect();
        Ring::new(7, mounted, not_fuse(), &qids, 1, 8192).unwrap()
    }

    /// For tests that assert the mapping is gone after drop: a 1 GiB stride keeps the small
    /// mappings of other tests away from `base`. `None` when the host refuses the reservation
    fn big_ring(n: u16, mounted: bool) -> Option<Arc<Ring>> {
        let qids: Vec<u16> = (0..n).collect();
        match Ring::new(7, mounted, not_fuse(), &qids, 1, 1 << 30) {
            Ok(ring) => Some(ring),
            Err(e) if e.raw_os_error() == Some(libc::ENOMEM) => {
                eprintln!("skipping: cannot reserve {n} GiB of address space: {e}");
                None
            }
            Err(e) => panic!("Ring::new: {e}"),
        }
    }

    /// `None` when the environment forbids io_uring or the kernel predates the setup flags
    fn try_ring_io(sq: u32, cq: u32) -> Option<RingIo> {
        match RingIo::open(sq, cq) {
            Ok(mut io) => {
                io.hooks.refusals_in_serve = true;
                Some(io)
            }
            Err(e) if matches!(e.raw_os_error(), Some(libc::EPERM | libc::ENOSYS)) => {
                eprintln!("skipping: io_uring_setup failed with {e}");
                None
            }
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) && flags_unsupported() => {
                eprintln!(
                    "skipping: io_uring_setup failed with {e}; SINGLE_ISSUER and DEFER_TASKRUN \
                     need Linux 6.1"
                );
                None
            }
            Err(e) => panic!("io_uring_setup: {e}"),
        }
    }

    /// Whether `io_uring_setup` refuses known-good sizes too, which tells a kernel without the
    /// setup flags apart from an `EINVAL` for the sizes under test
    fn flags_unsupported() -> bool {
        static PROBE: OnceLock<bool> = OnceLock::new();
        *PROBE.get_or_init(
            || matches!(RingIo::open(8, 16), Err(e) if e.raw_os_error() == Some(libc::EINVAL)),
        )
    }

    fn state_name(e: &RingEntry) -> &'static str {
        match &*e.state.lock() {
            EntryState::InKernel { .. } => "InKernel",
            EntryState::Dispatching { .. } => "Dispatching",
            EntryState::Deferred { .. } => "Deferred",
            EntryState::Dispatched { .. } => "Dispatched",
            EntryState::Committing => "Committing",
            EntryState::Pending { .. } => "Pending",
            EntryState::PendingRead { .. } => "PendingRead",
            EntryState::Reading { .. } => "Reading",
            EntryState::Dead => "Dead",
        }
    }

    fn last_command(e: &RingEntry) -> Option<u64> {
        match *e.state.lock() {
            EntryState::InKernel { last } => Some(last),
            _ => None,
        }
    }

    fn errno_of(err: &io::Error) -> Option<i32> {
        err.raw_os_error()
    }

    /// What `uring_cmd` on a file without that operation returns; kernels differ
    fn is_rejection(err: &io::Error) -> bool {
        matches!(
            errno_of(err),
            Some(libc::EOPNOTSUPP | libc::EINVAL | libc::ENOTTY)
        )
    }

    fn header_bytes(e: &RingEntry) -> [u8; HEADER_SZ] {
        // SAFETY: test-owned entry with no command pending.
        unsafe { ptr::read_unaligned(e.base.0.as_ptr().cast()) }
    }

    /// Writes what the kernel writes on fetch into a real entry
    fn fake_fetch(e: &RingEntry, unique: u64, opcode: fuse_opcode, op_in: &[u8], payload: &[u8]) {
        let len = (40 + op_in.len() + payload.len()) as u32;
        let header = in_header(len, opcode as u32, unique);
        let base = e.base.0.as_ptr();
        // SAFETY: test-owned entry with no command pending and no reference live.
        unsafe {
            ptr::copy_nonoverlapping(header.as_ptr(), base, header.len());
            ptr::copy_nonoverlapping(op_in.as_ptr(), base.add(OP_IN_OFFSET), op_in.len());
            ptr::write_unaligned(base.add(COMMIT_ID_OFFSET).cast::<u64>(), unique);
            ptr::write_unaligned(
                base.add(PAYLOAD_SZ_OFFSET).cast::<u32>(),
                payload.len() as u32,
            );
            ptr::copy_nonoverlapping(payload.as_ptr(), base.add(e.gap), payload.len());
        }
    }

    fn set_in_kernel(ring: &Ring, idx: usize, last: u64) {
        *ring.entries[idx].state.lock() = EntryState::InKernel { last };
        ring.live.lock().in_kernel += 1;
    }

    fn fake_dispatched(ring: &Arc<Ring>, idx: usize, commit_id: u64) -> RingCommit {
        *ring.entries[idx].state.lock() = EntryState::Dispatched { commit_id };
        ring.live.lock().outstanding += 1;
        RingCommit {
            ring: Arc::clone(ring),
            idx: idx as u32,
            commit_id,
        }
    }

    fn ok_header(unique: u64) -> abi::fuse_out_header {
        abi::fuse_out_header {
            len: 16,
            error: 0,
            unique,
        }
    }

    /// CONSTELLATION PATCH (io-uring): one request dispatched over a ring entry with **no
    /// io_uring and no kernel behind it** -- the entry is an ordinary private mapping, and the
    /// commit's SQE is queued in `Live::pending` and never submitted -- and the reply bytes the
    /// handler left in it, in the `/dev/fuse` wire shape (the 16-byte `fuse_out_header`
    /// followed by `payload_sz` payload bytes, which is exactly what the `writev(2)` of the
    /// other transport sends).
    ///
    /// `request` is the contiguous request the `/dev/fuse` parser is given (`fuse_in_header`,
    /// then `op_in_len` bytes of fixed op arguments, then the op's payload); it is scattered
    /// into the entry's header and payload areas the way the kernel scatters it, so that
    /// `stage_request` has to reassemble it.
    ///
    /// This is the in-memory seam plan 38 §6 asks for: it lets a test compare what a
    /// `Filesystem` answers over the two transports byte for byte, without root, a mount, or
    /// `fuse.enable_uring=Y`. `session.rs`'s `transport_parity_is_byte_for_byte` is the test.
    pub(crate) fn dispatch_over_a_fake_ring(
        request: &[u8],
        op_in_len: usize,
        handle: impl FnOnce(RingCommit, &[u8]),
    ) -> Vec<u8> {
        const IN_HEADER_SZ: usize = size_of::<abi::fuse_in_header>();
        let ring = fake_ring(1, true);
        // The commit path skips the eventfd kick for the ring's own thread, which is what a
        // dispatch from a fetch is; nothing here reads the eventfd.
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        let unique = u64_at(request, 8);
        let op_in = &request[IN_HEADER_SZ..IN_HEADER_SZ + op_in_len];
        let payload = &request[IN_HEADER_SZ + op_in_len..];
        assert!(payload.len() <= e.payload_cap, "the fixture's payload fits");
        let base = e.base.0.as_ptr();
        // SAFETY: a test-owned entry with no command pending and no reference into it live.
        // The header, `op_in` and the commit/payload-size fields lie inside the 288-byte
        // header area, and the payload fits `payload_cap`.
        unsafe {
            ptr::copy_nonoverlapping(request.as_ptr(), base, IN_HEADER_SZ);
            ptr::copy_nonoverlapping(op_in.as_ptr(), base.add(OP_IN_OFFSET), op_in_len);
            ptr::write_unaligned(base.add(COMMIT_ID_OFFSET).cast::<u64>(), unique);
            ptr::write_unaligned(
                base.add(PAYLOAD_SZ_OFFSET).cast::<u32>(),
                payload.len() as u32,
            );
            ptr::copy_nonoverlapping(payload.as_ptr(), base.add(e.gap), payload.len());
        }
        // Staged exactly as the ring thread stages a fetched entry.
        // SAFETY: as above.
        let staged = unsafe { stage_request(e.base.0, e.gap, e.payload_cap) }
            .expect("the fixture's request is well formed");
        let commit = fake_dispatched(&ring, 0, unique);
        // SAFETY: as above; the slice dies with this statement, before the reply is read.
        handle(commit, unsafe {
            slice::from_raw_parts(staged.req.as_ptr(), staged.len)
        });
        let (_, _, _, payload_sz) = reply_fields(e);
        let mut reply = header_bytes(e)[..OUT_HEADER_SZ].to_vec();
        reply.extend_from_slice(payload_bytes(e, payload_sz as usize));
        // The entry is never re-armed: let the ring drop without waiting for a CQE.
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
        reply
    }

    /// A handle whose commit is refused: `NotConnected` when `conn_dead`, else a duplicate
    pub(crate) fn refused_commit(conn_dead: bool) -> RingCommit {
        let ring = fake_ring(1, true);
        ring.live.lock().conn_dead = conn_dead;
        RingCommit {
            ring,
            idx: 0,
            commit_id: 7,
        }
    }

    /// A `Nop` whose CQE carries `res` (`IORING_NOP_INJECT_RESULT`, Linux 6.10+): `nop_flags`
    /// at byte 28, the result in `len` at byte 24. Older kernels ignore both fields
    fn nop_with_result(user_data: u64, res: i32) -> squeue::Entry128 {
        let nop: squeue::Entry128 = opcode::Nop::new().build().user_data(user_data).into();
        let mut raw = sqe_bytes(nop);
        raw[24..28].copy_from_slice(&(res as u32).to_ne_bytes());
        raw[28..32].copy_from_slice(&1u32.to_ne_bytes());
        sqe_from_bytes(raw)
    }

    /// Whether this kernel honours `IORING_NOP_INJECT_RESULT`; before 6.10 the Nop completes
    /// with 0 as if the flag were not there
    fn nop_results_supported(io: &mut RingIo) -> bool {
        io.push_or_submit(&nop_with_result(WAKE - 1, -42)).unwrap();
        io.uring().submit_and_wait(1).unwrap();
        let res = io.uring().completion().next().unwrap().result();
        match res {
            -42 => true,
            0 => {
                eprintln!("skipping: the kernel does not support IORING_NOP_INJECT_RESULT");
                false
            }
            r => panic!("nop result {r}"),
        }
    }

    /// Spawns `thread_main` on `ring` and drives it to the registered state
    struct Started {
        thread: thread::JoinHandle<io::Result<()>>,
        handler_tx: mpsc::Sender<Box<dyn FetchHandler>>,
        registered: mpsc::Receiver<io::Result<()>>,
        go: mpsc::Sender<()>,
    }

    fn start(ring: &Arc<Ring>, io: RingIo) -> Started {
        let (go, go_rx) = mpsc::channel();
        let (reg_tx, registered) = mpsc::channel();
        let (handler_tx, handler_rx) = mpsc::channel::<Box<dyn FetchHandler>>();
        let thread = {
            let ring = Arc::clone(ring);
            thread::spawn(move || ring.thread_main(io, go_rx, reg_tx, handler_rx))
        };
        Started {
            thread,
            handler_tx,
            registered,
            go,
        }
    }

    impl Started {
        fn registered(&self) {
            self.go.send(()).unwrap();
            self.registered
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
        }
    }

    #[test]
    fn entry128_layout_and_len_patch() {
        assert_eq!(size_of::<squeue::Entry128>(), 128);
        let sqe = opcode::UringCmd80::new(types::Fd(5), 1)
            .cmd([0xAB; 80])
            .addr(Some(0x1122_3344_5566_7788))
            .build()
            .user_data(0x0102_0304_0506_0708);
        let before = sqe_bytes(sqe.clone());
        assert_eq!(u32_at(&before, SQE_LEN_OFFSET), 0);
        let after = sqe_bytes(set_sqe_len(sqe, 2));
        assert_eq!(u32_at(&after, SQE_LEN_OFFSET), 2);
        for (i, (a, b)) in before.iter().zip(after.iter()).enumerate() {
            if !(SQE_LEN_OFFSET..SQE_LEN_OFFSET + 4).contains(&i) {
                assert_eq!(a, b, "byte {i}");
            }
        }
        assert_eq!(after[0], 46, "IORING_OP_URING_CMD");
        assert_eq!(u32_at(&after, 4), 5, "fd");
        assert_eq!(u32_at(&after, 8), 1, "cmd_op");
        assert_eq!(u64_at(&after, 16), 0x1122_3344_5566_7788, "addr");
        assert_eq!(u64_at(&after, 32), 0x0102_0304_0506_0708, "user_data");
        assert_eq!(&after[48..128], &[0xAB; 80][..], "cmd");
    }

    #[test]
    fn user_data_round_trips_and_never_collides_with_wake() {
        for (qid, idx) in [(0, 0), (447, 3583), (u16::MAX, u32::MAX), (1, u32::MAX)] {
            let ud = user_data(qid, idx);
            assert_eq!(decode(ud), (qid, idx));
            assert_ne!(ud, WAKE);
        }
    }

    #[test]
    fn register_and_commit_sqe_encoding() {
        let ring = fake_ring(3, true);
        let e = &ring.entries[2];
        assert_eq!((e.qid, e.idx), (2, 2));
        let fd = ring.device.as_raw_fd() as u32;

        let reg = sqe_bytes(ring.register_sqe(e));
        assert_eq!(reg[0], 46);
        assert_eq!(reg[1], 0, "no IOSQE flags");
        assert_eq!(u32_at(&reg, 4), fd);
        assert_eq!(u32_at(&reg, 8), 1, "REGISTER");
        assert_eq!(u64_at(&reg, 16), e.iov.0.as_ptr() as u64);
        assert_eq!(u32_at(&reg, 24), 2, "len is the iovec count");
        assert_eq!(u64_at(&reg, 32), user_data(2, 2));
        // cmd: fuse_uring_cmd_req { flags 0, commit_id 0, qid 2 }, rest zero
        assert_eq!(u64_at(&reg, 48), 0);
        assert_eq!(u64_at(&reg, 56), 0);
        assert_eq!(u16_at(&reg, 64), 2);
        assert!(reg[66..128].iter().all(|b| *b == 0));
        assert_eq!(e.iov.0[0].iov_base, e.base.0.as_ptr().cast());
        assert_eq!(e.iov.0[0].iov_len, 288);
        assert_eq!(
            e.iov.0[1].iov_base as usize,
            e.base.0.as_ptr() as usize + e.gap
        );
        assert_eq!(e.iov.0[1].iov_len, ring.mem.payload_cap());
        assert!(ring.mem.payload_cap() >= 8192);
        assert_eq!(e.gap, page_size::get());
        assert_eq!(ring.reserved_bytes(), ring.mem.len());

        let commit = sqe_bytes(ring.commit_sqe(e, 0xDEAD_BEEF_0000_0042));
        assert_eq!(commit[0], 46);
        assert_eq!(u32_at(&commit, 4), fd);
        assert_eq!(u32_at(&commit, 8), 2, "COMMIT_AND_FETCH");
        assert_eq!(u64_at(&commit, 16), 0, "no addr");
        assert_eq!(u32_at(&commit, 24), 0, "no len");
        assert_eq!(u64_at(&commit, 32), user_data(2, 2));
        assert_eq!(u64_at(&commit, 48), 0);
        assert_eq!(u64_at(&commit, 56), 0xDEAD_BEEF_0000_0042);
        assert_eq!(u16_at(&commit, 64), 2);
        assert!(commit[66..128].iter().all(|b| *b == 0));

        let wake = sqe_bytes(ring.wake_sqe());
        assert_eq!(wake[0], 6, "IORING_OP_POLL_ADD");
        assert_eq!(u32_at(&wake, 4), ring.wake.as_raw_fd() as u32);
        assert_eq!(u32_at(&wake, 24) & 1, 1, "IORING_POLL_ADD_MULTI");
        assert_eq!(u32_at(&wake, 28), libc::POLLIN as u32, "poll32_events");
        assert_eq!(u64_at(&wake, 32), WAKE);
    }

    #[test]
    fn ring_new_rejects_empty_and_oversized_rings() {
        assert!(Ring::new(0, true, not_fuse(), &[], 1, 8192).is_err());
        assert!(Ring::new(0, true, not_fuse(), &[0, 1], 0, 8192).is_err());
        let all: Vec<u16> = (0..=u16::MAX).collect();
        assert!(Ring::new(0, true, not_fuse(), &all, u32::MAX, 8192).is_err());
    }

    #[test]
    fn sizes_follow_the_kernel_limits() {
        for entries in [1, 2, 3, 8, 9, 3584, 32768, 40_000, 100_000] {
            let (sq, cq) = ring_sizes(entries);
            assert!(sq.is_power_of_two(), "{entries}");
            assert!((8..=32768).contains(&sq), "{entries}");
            assert!(cq >= sq, "{entries}: cq {cq} < sq {sq}");
            assert!(cq <= 65536, "{entries}");
            assert!(cq as usize >= (2 * entries).min(65536), "{entries}");
        }
        assert_eq!(ring_sizes(2), (8, 8));
        assert_eq!(ring_sizes(3584), (4096, 8192));
        assert_eq!(ring_sizes(100_000), (32768, 65536));
    }

    /// The one test that fails rather than skips when io_uring is unavailable
    #[test]
    fn io_uring_is_available() {
        RingIo::open(8, 16).expect(
            "io_uring_setup failed (needs Linux 6.1 and no seccomp/sysctl ban); the other ring \
             tests are skipping",
        );
    }

    #[test]
    fn small_rings_open() {
        for entries in [1, 2, 3] {
            let (sq, cq) = ring_sizes(entries);
            if try_ring_io(sq, cq).is_none() {
                return;
            }
        }
    }

    #[test]
    fn enabled_ring_submits_only_for_the_enabling_thread() {
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        io.enable().unwrap();
        let (err, mut io) = thread::spawn(move || {
            let nop: squeue::Entry128 = opcode::Nop::new().build().user_data(WAKE - 1).into();
            io.push_or_submit(&nop).unwrap();
            (io.uring().submit().unwrap_err(), io)
        })
        .join()
        .unwrap();
        assert_eq!(errno_of(&err), Some(libc::EEXIST), "{err}");
        assert_eq!(io.uring().submit_and_wait(1).unwrap(), 1);
        let cqe = io.uring().completion().next().unwrap();
        assert_eq!((cqe.user_data(), cqe.result()), (WAKE - 1, 0));
    }

    #[test]
    fn ring_rejects_submission_until_enabled() {
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        let nop: squeue::Entry128 = opcode::Nop::new().build().user_data(WAKE - 1).into();
        io.push_or_submit(&nop).unwrap();
        let err = io.uring().submit().unwrap_err();
        assert_eq!(errno_of(&err), Some(libc::EBADFD), "{err}");
    }

    #[test]
    fn ring_mechanics_against_a_non_fuse_device() {
        let _serial = UNMAP_CHECK.lock();
        let Some(io) = try_ring_io(8, 16) else { return };
        let Some(ring) = big_ring(2, true) else {
            return;
        };
        let base = ring.mem.entry(0).as_ptr() as usize;
        let started = start(&ring, io);
        started.registered();
        started
            .handler_tx
            .send(Box::new(|_: RingCommit, _: &[u8]| {
                panic!("nothing is fetched from {NOT_FUSE}")
            }))
            .unwrap();
        let err = started.thread.join().unwrap().unwrap_err();
        assert!(is_rejection(&err), "{err}");
        let live = ring.live.lock();
        assert!(live.exited);
        assert_eq!(live.in_kernel, 0);
        assert_eq!(live.outstanding, 0);
        assert!(live.pending.is_empty());
        assert!(live.fatal.is_none(), "taken by thread_main");
        assert!(!live.conn_dead);
        drop(live);
        for e in &ring.entries {
            assert_eq!(state_name(e), "Dead");
        }
        assert!(ring.ring_thread.get().is_some());
        assert!(is_mapped(base));
        drop(ring);
        assert!(!is_mapped(base));
    }

    #[test]
    fn dropped_go_registers_nothing() {
        let _serial = UNMAP_CHECK.lock();
        let Some(io) = try_ring_io(8, 16) else { return };
        let Some(ring) = big_ring(2, true) else {
            return;
        };
        let base = ring.mem.entry(0).as_ptr() as usize;
        let started = start(&ring, io);
        drop(started.go);
        started.thread.join().unwrap().unwrap();
        assert!(
            started.registered.try_recv().is_err(),
            "nothing was registered"
        );
        assert_eq!(ring.live.lock().in_kernel, 0);
        drop(ring);
        assert!(!is_mapped(base));
    }

    #[test]
    fn failed_enable_registers_nothing() {
        let _serial = UNMAP_CHECK.lock();
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        let Some(ring) = big_ring(2, true) else {
            return;
        };
        let base = ring.mem.entry(0).as_ptr() as usize;
        io.hooks.fail_enable = Some(libc::EBADFD);
        let started = start(&ring, io);
        started.go.send(()).unwrap();
        let err = started
            .registered
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err();
        assert_eq!(errno_of(&err), Some(libc::EBADFD));
        started.thread.join().unwrap().unwrap();
        assert_eq!(ring.live.lock().in_kernel, 0);
        for e in &ring.entries {
            assert_eq!(state_name(e), "Dead", "register_all never ran");
        }
        drop(ring);
        assert!(!is_mapped(base));
    }

    /// The commands stay counted, so the mapping is leaked on purpose
    #[test]
    fn dropped_from_fd_session_abandons_the_ring() {
        let Some(io) = try_ring_io(8, 16) else { return };
        let ring = fake_ring(2, false);
        let base = ring.mem.entry(0).as_ptr() as usize;
        let started = start(&ring, io);
        started.registered();
        drop(started.handler_tx);
        started.thread.join().unwrap().unwrap();
        assert_eq!(ring.live.lock().in_kernel, 2);
        drop(ring);
        assert!(is_mapped(base), "leaked on purpose");
    }

    /// The commands stay counted, so the mapping is leaked on purpose
    #[test]
    fn failed_start_abandons_a_registered_ring() {
        let Some(io) = try_ring_io(8, 16) else { return };
        let ring = fake_ring(2, true);
        let base = ring.mem.entry(0).as_ptr() as usize;
        let started = start(&ring, io);
        started.registered();
        ring.abandon();
        drop(started.handler_tx);
        started.thread.join().unwrap().unwrap();
        assert!(!ring.live.lock().exited, "serve never ran");
        assert_eq!(ring.live.lock().in_kernel, 2);
        drop(ring);
        assert!(is_mapped(base), "leaked on purpose");
    }

    /// A `Session::new` session dropped before `run` serves EIO until the connection ends
    #[test]
    fn dropped_mounted_session_drains() {
        let Some(io) = try_ring_io(8, 16) else { return };
        let ring = fake_ring(2, true);
        let started = start(&ring, io);
        started.registered();
        drop(started.handler_tx);
        let err = started.thread.join().unwrap().unwrap_err();
        assert!(is_rejection(&err), "{err}");
        assert!(ring.live.lock().exited);
        assert_eq!(ring.live.lock().in_kernel, 0);
    }

    #[test]
    fn push_or_submit_batches_when_the_queue_is_full() {
        let Some(mut io) = try_ring_io(8, 64) else {
            return;
        };
        io.enable().unwrap();
        let ring = fake_ring(20, true);
        ring.register_all(&mut io).unwrap();
        // 20 REGISTERs and the poll: the queue fills after 8 and 16, then the final submit
        assert_eq!(io.hooks.submits, 3);
        assert_eq!(io.uring().submission().len(), 0);
        assert_eq!(ring.live.lock().in_kernel, 20);
        for e in &ring.entries {
            assert_eq!(last_command(e), Some(0));
        }
    }

    /// Runs `serve` on its own thread with `in_kernel` inflated by one so the ring never
    /// exits on its own; the test ends it by zeroing the count and waking the thread
    struct Served {
        ring: Arc<Ring>,
        thread: thread::JoinHandle<(io::Result<()>, RingIo)>,
        flushed: mpsc::Receiver<u32>,
    }

    impl Served {
        fn start(mut io: RingIo, ring: Arc<Ring>, poll_multishot: bool) -> Self {
            let (tx, flushed) = mpsc::channel();
            *ring.hooks.flushed.lock() = Some(tx);
            ring.live.lock().in_kernel = 1;
            let sqe: squeue::Entry128 =
                opcode::PollAdd::new(types::Fd(ring.wake.as_raw_fd()), libc::POLLIN as u32)
                    .multi(poll_multishot)
                    .build()
                    .user_data(WAKE)
                    .into();
            io.push_or_submit(&sqe).unwrap();
            let thread = {
                let ring = Arc::clone(&ring);
                thread::spawn(move || {
                    ring.ring_thread.set(thread::current().id()).ok();
                    io.enable().unwrap();
                    let mut handler = |_: RingCommit, _: &[u8]| panic!("nothing is fetched");
                    let outcome = ring.serve(&mut io, &mut handler);
                    (outcome, io)
                })
            };
            Self {
                ring,
                thread,
                flushed,
            }
        }

        /// Commits from a foreign thread and waits for the ring thread to flush it
        fn foreign_commit(&self, commit: RingCommit) {
            let header = ok_header(commit.commit_id);
            let expected = commit.idx;
            thread::spawn(move || commit.commit(&[IoSlice::new(header.as_bytes())]))
                .join()
                .unwrap()
                .unwrap();
            let idx = self.flushed.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(idx, expected);
        }

        fn wait_retired(&self, idx: usize) {
            let deadline = Instant::now() + Duration::from_secs(2);
            while state_name(&self.ring.entries[idx]) != "Dead" {
                assert!(Instant::now() < deadline, "entry {idx} never retired");
                thread::sleep(Duration::from_millis(1));
            }
        }

        fn wake(&self) {
            self.ring.wake.write(1).unwrap();
        }

        fn finish(self) -> (io::Result<()>, RingIo) {
            {
                let mut live = self.ring.live.lock();
                live.in_kernel = 0;
                live.fatal.get_or_insert(io::Error::other("test over"));
            }
            self.wake();
            let (outcome, io) = self.thread.join().unwrap();
            assert!(self.ring.live.lock().exited);
            assert!(self.ring.live.lock().pending.is_empty());
            (outcome, io)
        }
    }

    #[test]
    fn eventfd_wakes_the_ring_thread() {
        let Some(io) = try_ring_io(8, 16) else { return };
        let ring = fake_ring(3, true);
        // A one-shot poll: its CQE lacks IORING_CQE_F_MORE, so the loop must re-arm
        let served = Served::start(io, ring, false);

        served.foreign_commit(fake_dispatched(&served.ring, 0, 41));
        assert!(
            served.flushed.try_recv().is_err(),
            "exactly one commit pushed"
        );
        served.wait_retired(0);
        // The commit went to a non-FUSE device, so it was rejected and the count is back to one
        assert_eq!(served.ring.live.lock().in_kernel, 1);
        assert_eq!(served.ring.live.lock().outstanding, 0);
        assert!(is_rejection(
            served.ring.live.lock().fatal.as_ref().unwrap()
        ));

        // The poll was re-armed (now multishot): a second foreign commit still wakes it
        served.foreign_commit(fake_dispatched(&served.ring, 1, 42));
        served.wait_retired(1);

        let bytes = header_bytes(&served.ring.entries[1]);
        assert_eq!(u32_at(&bytes, 0), 16);
        assert_eq!(u32_at(&bytes, 4), 0);
        assert_eq!(u64_at(&bytes, 8), 42);
        assert_eq!(u64_at(&bytes, FLAGS_OFFSET), 0);
        assert_eq!(u32_at(&bytes, PAYLOAD_SZ_OFFSET), 0);

        let (outcome, mut io) = served.finish();
        outcome.unwrap();
        assert!(io.uring().submission().is_empty());
    }

    /// CONSTELLATION PATCH (io-uring): a reply handed to an idle ring thread whose eventfd
    /// announcement is lost still reaches the kernel: the ring thread's wait is bounded
    /// (`IDLE_WAIT`), the next pass flushes it, and it is counted as stranded. Before the
    /// bound the thread slept until something else woke it -- in a ring nobody else uses, never
    /// (the signature of overload-cascade-2's `gate1` hang: the caller waited in the kernel,
    /// the daemon had nothing in flight).
    #[test]
    fn a_reply_whose_wake_up_is_lost_is_flushed_by_the_idle_wait() {
        let Some(io) = try_ring_io(8, 16) else { return };
        let ring = fake_ring(3, true);
        let health = crate::uring::RingHealth::new(Duration::from_secs(30));
        ring.set_health(health.clone());
        let served = Served::start(io, ring, true);
        served.ring.hooks.lost_wakes.store(1, Ordering::SeqCst);
        // Commit right after the ring thread started a wait, so that the reply is older than
        // `STRANDED_AFTER_MS` when that wait times out
        let passes = served.ring.hooks.exit_checks.load(Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(3);
        while served.ring.hooks.exit_checks.load(Ordering::SeqCst) == passes {
            assert!(Instant::now() < deadline, "the ring thread never waited again");
            thread::sleep(Duration::from_millis(1));
        }
        let commit = fake_dispatched(&served.ring, 0, 71);
        let header = ok_header(71);
        thread::spawn(move || commit.commit(&[IoSlice::new(header.as_bytes())]))
            .join()
            .unwrap()
            .unwrap();
        assert_eq!(served.ring.hooks.lost_wakes.load(Ordering::SeqCst), 0);
        let idx = served
            .flushed
            .recv_timeout(Duration::from_secs(5))
            .expect("a reply whose wake-up was lost was never flushed");
        assert_eq!(idx, 0);
        served.wait_retired(0);
        assert_eq!(health.stranded_commits(), 1);

        // An announced reply is not counted
        served.foreign_commit(fake_dispatched(&served.ring, 1, 72));
        served.wait_retired(1);
        thread::sleep(IDLE_WAIT + Duration::from_millis(200));
        assert_eq!(health.stranded_commits(), 1);

        let (outcome, _io) = served.finish();
        outcome.unwrap();
    }

    /// CONSTELLATION PATCH (io-uring): `report_held` counts the entries userspace has held past
    /// the threshold, blocking lock requests aside, and leaves the rest alone.
    #[test]
    fn report_held_counts_long_held_entries_but_not_lock_waits() {
        let ring = fake_ring(4, true);
        let now = mono_ms();
        let fetched = |idx: usize, unique: u64, age_ms: u64| {
            let e = &ring.entries[idx];
            e.fetched_unique.store(unique, Ordering::Relaxed);
            e.fetched_ms.store(now - age_ms, Ordering::Relaxed);
        };
        // Held for a minute: counted
        let _held = fake_dispatched(&ring, 0, 81);
        fetched(0, 81, 60_000);
        // Held for a moment: not yet
        let _young = fake_dispatched(&ring, 1, 82);
        fetched(1, 82, 10);
        // A blocking lock request held for a minute: by design, not counted
        let _lock = fake_dispatched(&ring, 2, 83);
        fetched(2, 83, 60_000);
        ring.entries[2].lock_wait.store(true, Ordering::Relaxed);
        // Back in the kernel after a long fetch: not held at all
        *ring.entries[3].state.lock() = EntryState::InKernel { last: 84 };
        fetched(3, 84, 60_000);
        assert_eq!(ring.report_held(30_000), 1);
        // Logged once per fetch, counted every pass
        assert_eq!(ring.entries[0].held_reported.load(Ordering::Relaxed), 81);
        assert_eq!(ring.report_held(30_000), 1);
        assert_eq!(ring.report_held(5), 2);
        ring.live.lock().outstanding = 0;
    }

    #[test]
    fn cancelled_wake_poll_degrades_to_timed_waits() {
        let Some(io) = try_ring_io(8, 16) else { return };
        let ring = fake_ring(3, true);
        let served = Served::start(io, ring, true);
        // The cancel's own CQE must not look like an entry
        let cancel = || -> squeue::Entry128 {
            opcode::AsyncCancel::new(WAKE)
                .build()
                .user_data(user_data(u16::MAX, u32::MAX - 1))
                .into()
        };
        let poll_failures = |n: usize| {
            let deadline = Instant::now() + Duration::from_secs(2);
            while served.ring.hooks.poll_failed.load(Ordering::Relaxed) < n {
                assert!(Instant::now() < deadline, "poll failure {n} never arrived");
                thread::sleep(Duration::from_millis(1));
            }
        };

        // The poll is re-armed once; a foreign commit still wakes
        served.ring.hooks.inject.lock().push(cancel());
        served.wake();
        poll_failures(1);
        assert!(
            served.ring.live.lock().fatal.is_none(),
            "one re-arm is not fatal"
        );
        served.foreign_commit(fake_dispatched(&served.ring, 0, 51));
        served.wait_retired(0);

        // The poll is now dead; the wait is timed and commits still flush
        served.ring.hooks.inject.lock().push(cancel());
        served.wake();
        poll_failures(2);
        served.foreign_commit(fake_dispatched(&served.ring, 1, 52));
        served.wait_retired(1);

        let (outcome, _io) = served.finish();
        outcome.unwrap();
    }

    /// Leaving earlier would drop the held reply
    #[test]
    fn fatal_does_not_exit_while_a_request_is_outstanding() {
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        let ring = fake_ring(1, true);
        let commit = fake_dispatched(&ring, 0, 61);
        ring.live.lock().fatal = Some(io::Error::other("preset"));
        io.push_or_submit(&ring.wake_sqe()).unwrap();
        let (tx, flushed) = mpsc::channel();
        *ring.hooks.flushed.lock() = Some(tx);
        let server = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                ring.ring_thread.set(thread::current().id()).ok();
                io.enable().unwrap();
                let mut handler = |_: RingCommit, _: &[u8]| {};
                ring.serve(&mut io, &mut handler)
            })
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        while ring.hooks.exit_checks.load(Ordering::Relaxed) == 0 {
            assert!(
                Instant::now() < deadline,
                "serve never reached the exit test"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert!(!server.is_finished(), "exited with a request outstanding");
        assert!(!ring.live.lock().exited);

        let header = ok_header(61);
        commit.commit(&[IoSlice::new(header.as_bytes())]).unwrap();
        assert_eq!(flushed.recv_timeout(Duration::from_secs(2)).unwrap(), 0);
        server.join().unwrap().unwrap();
        let live = ring.live.lock();
        assert!(live.exited);
        assert_eq!((live.in_kernel, live.outstanding), (0, 0));
        drop(live);
        assert_eq!(state_name(&ring.entries[0]), "Dead");
    }

    /// With every entry held by userspace nothing in the kernel can ever complete
    #[test]
    fn shutdown_ends_a_ring_whose_entries_are_all_held() {
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        let ring = fake_ring(2, true);
        let held = [fake_dispatched(&ring, 0, 61), fake_dispatched(&ring, 1, 62)];
        io.push_or_submit(&ring.wake_sqe()).unwrap();
        let server = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                ring.ring_thread.set(thread::current().id()).ok();
                io.enable().unwrap();
                let mut handler = |_: RingCommit, _: &[u8]| {};
                ring.serve(&mut io, &mut handler)
            })
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        while ring.hooks.exit_checks.load(Ordering::Relaxed) == 0 {
            assert!(
                Instant::now() < deadline,
                "serve never reached the exit test"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert!(!server.is_finished(), "exited with requests outstanding");

        let asked = Instant::now();
        ring.shutdown();
        while !server.is_finished() {
            assert!(
                asked.elapsed() < Duration::from_secs(2),
                "exit was not prompt"
            );
            thread::sleep(Duration::from_millis(1));
        }
        server.join().unwrap().unwrap();
        let live = ring.live.lock();
        assert!(live.exited && live.shutdown && !live.conn_dead);
        assert_eq!((live.in_kernel, live.outstanding), (0, 2));
        drop(live);
        // Late replies to the stranded requests are dropped without touching the buffers
        for commit in &held {
            let header = ok_header(commit.commit_id);
            commit.commit(&[IoSlice::new(header.as_bytes())]).unwrap();
        }
        assert_eq!(ring.live.lock().outstanding, 0);
        assert!(ring.live.lock().pending.is_empty());
        for e in &ring.entries {
            assert_eq!(state_name(e), "Dead");
            assert!(header_bytes(e).iter().all(|b| *b == 0));
        }
    }

    /// CQE results injected with `IORING_NOP_INJECT_RESULT`
    #[test]
    fn cqe_classification() {
        let Some(mut io) = try_ring_io(16, 32) else {
            return;
        };
        io.enable().unwrap();
        if !nop_results_supported(&mut io) {
            return;
        }
        let ring = fake_ring(6, true);
        for (idx, last) in [(0, 0), (1, 5), (2, 0), (3, 9), (4, 0), (5, 0)] {
            set_in_kernel(&ring, idx, last);
        }
        // Entries 0 and 5 hold a parseable request, so a wrongly accepted unknown CQE with
        // `res == 0` would reach the panicking handler instead of failing to stage
        fake_fetch(
            &ring.entries[0],
            100,
            fuse_opcode::FUSE_GETATTR,
            &[0; 16],
            &[],
        );
        fake_fetch(
            &ring.entries[5],
            105,
            fuse_opcode::FUSE_GETATTR,
            &[0; 16],
            &[],
        );
        io.push_or_submit(&ring.wake_sqe()).unwrap();
        let cqes = [
            (user_data(0, 0), -libc::EAGAIN),
            (user_data(1, 1), -libc::EINTR),
            (user_data(2, 2), -libc::ENOENT),
            (user_data(3, 3), -libc::ECANCELED),
            (user_data(4, 4), 7),
            // Unknown entries: index out of range, and valid indexes under the wrong qid
            (user_data(0, 6), 0),
            (user_data(u16::MAX, 0), 0),
            (user_data(0, 5), 0),
            (user_data(5, 5), -libc::ECONNABORTED),
        ];
        for (ud, res) in cqes {
            io.push_or_submit(&nop_with_result(ud, res)).unwrap();
        }
        let mut handler = |_: RingCommit, _: &[u8]| panic!("no entry was fetched");
        ring.ring_thread.set(thread::current().id()).ok();
        ring.serve(&mut io, &mut handler).unwrap();
        let live = ring.live.lock();
        assert!(live.exited);
        assert!(live.conn_dead);
        assert_eq!((live.in_kernel, live.outstanding), (0, 0));
        let fatal = live.fatal.as_ref().unwrap();
        assert!(
            is_rejection(fatal)
                || matches!(errno_of(fatal), Some(libc::ENOENT | libc::ECANCELED))
                || fatal.to_string() == "unexpected result 7",
            "{fatal}"
        );
        drop(live);
        for e in &ring.entries {
            assert_eq!(state_name(e), "Dead");
        }
        let mut resubmitted = ring.hooks.resubmitted.lock().clone();
        resubmitted.sort_unstable();
        assert_eq!(
            resubmitted,
            [(0, 0), (1, 5)],
            "REGISTER for 0, COMMIT_AND_FETCH for 1"
        );
        assert_eq!(
            ring.hooks.ignored.load(Ordering::Relaxed),
            3,
            "unknown CQEs dropped"
        );
    }

    #[test]
    fn cqe_for_an_entry_not_in_the_kernel_is_ignored() {
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        io.enable().unwrap();
        if !nop_results_supported(&mut io) {
            return;
        }
        let ring = fake_ring(2, true);
        set_in_kernel(&ring, 1, 0);
        // Entry 0 is Dispatched (held by the filesystem), with a parseable request in place
        let _held = fake_dispatched(&ring, 0, 200);
        fake_fetch(
            &ring.entries[0],
            200,
            fuse_opcode::FUSE_GETATTR,
            &[0; 16],
            &[],
        );
        io.push_or_submit(&ring.wake_sqe()).unwrap();
        io.push_or_submit(&nop_with_result(user_data(0, 0), 0))
            .unwrap();
        io.push_or_submit(&nop_with_result(user_data(0, 0), -libc::ENOENT))
            .unwrap();
        io.push_or_submit(&nop_with_result(user_data(1, 1), -libc::ENOTCONN))
            .unwrap();
        let mut handler = |_: RingCommit, _: &[u8]| panic!("no counted entry was fetched");
        ring.ring_thread.set(thread::current().id()).ok();
        ring.serve(&mut io, &mut handler).unwrap();
        let live = ring.live.lock();
        assert!(live.exited && live.conn_dead);
        assert_eq!((live.in_kernel, live.outstanding), (0, 1));
        assert!(live.fatal.is_none(), "the stray ENOENT was not classified");
        drop(live);
        assert_eq!(state_name(&ring.entries[0]), "Dispatched");
        assert_eq!(ring.hooks.ignored.load(Ordering::Relaxed), 2);
        ring.live.lock().outstanding = 0;
    }

    #[test]
    fn enotconn_is_a_clean_end() {
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        io.enable().unwrap();
        if !nop_results_supported(&mut io) {
            return;
        }
        let ring = fake_ring(2, true);
        set_in_kernel(&ring, 0, 0);
        set_in_kernel(&ring, 1, 3);
        io.push_or_submit(&ring.wake_sqe()).unwrap();
        io.push_or_submit(&nop_with_result(user_data(0, 0), -libc::ENOTCONN))
            .unwrap();
        io.push_or_submit(&nop_with_result(user_data(1, 1), -libc::ENOTCONN))
            .unwrap();
        let mut handler = |_: RingCommit, _: &[u8]| panic!("no entry was fetched");
        ring.ring_thread.set(thread::current().id()).ok();
        ring.serve(&mut io, &mut handler).unwrap();
        let live = ring.live.lock();
        assert!(live.exited && live.conn_dead);
        assert!(live.fatal.is_none());
        assert_eq!(live.in_kernel, 0);
    }

    #[test]
    fn fail_entry_keeps_the_first_error() {
        let ring = fake_ring(4, true);
        for idx in 0..4 {
            set_in_kernel(&ring, idx, 0);
        }
        ring.fail_entry(&ring.entries[0], -libc::ENOENT, 0);
        assert_eq!(state_name(&ring.entries[0]), "Dead");
        assert_eq!(ring.live.lock().in_kernel, 3);
        assert_eq!(
            errno_of(ring.live.lock().fatal.as_ref().unwrap()),
            Some(libc::ENOENT)
        );
        ring.fail_entry(&ring.entries[1], -libc::ECANCELED, 0);
        ring.fail_entry(&ring.entries[2], -libc::EINVAL, 0);
        ring.fail_entry(&ring.entries[3], 12, 0);
        assert_eq!(ring.live.lock().in_kernel, 0);
        assert_eq!(
            errno_of(ring.live.lock().fatal.as_ref().unwrap()),
            Some(libc::ENOENT)
        );
        for e in &ring.entries {
            assert_eq!(state_name(e), "Dead");
        }
        let ring = fake_ring(1, true);
        set_in_kernel(&ring, 0, 0);
        ring.fail_entry(&ring.entries[0], 12, 4);
        assert_eq!(
            ring.live.lock().fatal.as_ref().unwrap().to_string(),
            "unexpected result 12"
        );
        // A CQE for an entry that is not counted does not underflow the count
        ring.retire(&ring.entries[0], None);
        assert_eq!(ring.live.lock().in_kernel, 0);
    }

    /// The commit from inside the handler must not self-deadlock
    #[test]
    fn deferred_reply_is_written_after_dispatch() {
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let ring = fake_ring(1, true);
            ring.ring_thread.set(thread::current().id()).ok();
            let e = &ring.entries[0];
            fake_fetch(e, 9, fuse_opcode::FUSE_LOOKUP, &[], b"hello\0");
            set_in_kernel(&ring, 0, 0);

            let mut handler = |commit: RingCommit, request: &[u8]| {
                let req = AnyRequest::try_from(request).unwrap();
                assert_eq!(req.unique().0, 9);
                match req.operation().unwrap() {
                    Operation::Lookup(l) => assert_eq!(l.name().to_str().unwrap(), "hello"),
                    other => panic!("{other:?}"),
                }
                // Dispatching { direct_ok: false } while the name is borrowed
                assert_eq!(state_name(&commit.ring.entries[0]), "Dispatching");
                assert_eq!(commit.ring.live.lock().in_kernel, 0);
                assert_eq!(commit.ring.live.lock().outstanding, 1);
                let header = abi::fuse_out_header {
                    len: 16 + 8,
                    error: 0,
                    unique: 9,
                };
                commit
                    .commit(&[
                        IoSlice::new(header.as_bytes()),
                        IoSlice::new(b"abcd"),
                        IoSlice::new(b"efgh"),
                    ])
                    .unwrap();
                assert_eq!(state_name(&commit.ring.entries[0]), "Deferred");
                // Nothing was written yet: the request bytes are intact
                assert_eq!(&request[40..], b"hello\0");
            };
            ring.handle_fetch(e, &mut handler);

            assert_eq!(state_name(e), "Pending");
            let live = ring.live.lock();
            assert_eq!((live.in_kernel, live.outstanding), (1, 0));
            assert_eq!(live.pending, [0]);
            drop(live);
            let bytes = header_bytes(e);
            assert_eq!(u32_at(&bytes, 0), 24);
            assert_eq!(u32_at(&bytes, 4), 0);
            assert_eq!(u64_at(&bytes, 8), 9);
            assert_eq!(u32_at(&bytes, PAYLOAD_SZ_OFFSET), 8);
            // SAFETY: test-owned entry, no command pending.
            let payload = unsafe { slice::from_raw_parts(e.base.0.as_ptr().add(e.gap), 8) };
            assert_eq!(payload, b"abcdefgh");
            ring.live.lock().in_kernel = 0;
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handle_fetch deadlocked or panicked");
    }

    /// CONSTELLATION PATCH (io-uring): a held fetch leaves the ring thread at once; a reply
    /// made while it is held -- even one without a payload, from another thread -- is stashed
    /// rather than written, so the entry is not re-armed under the held slice, and goes out
    /// when the `HeldRequest` is dropped.
    #[test]
    fn a_held_fetch_finishes_where_it_is_dropped() {
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let ring = fake_ring(1, true);
            ring.ring_thread.set(thread::current().id()).ok();
            let e = &ring.entries[0];
            // No payload: an unheld fetch of it could be answered directly
            fake_fetch(e, 11, fuse_opcode::FUSE_GETATTR, &[0u8; 16], &[]);
            set_in_kernel(&ring, 0, 0);
            let mut held = None;
            let mut handler = |commit: RingCommit, request: &[u8]| {
                held = Some(commit.hold(request));
            };
            ring.handle_fetch(e, &mut handler);
            let held = held.unwrap();
            // The ring thread is done with it, the entry is not
            assert_eq!(state_name(e), "Dispatching");
            assert_eq!(ring.live.lock().outstanding, 1);
            let before = held.request().to_vec();
            let commit = held.commit().clone();
            thread::spawn(move || commit.commit_errno(Errno::ENOENT))
                .join()
                .unwrap();
            assert_eq!(state_name(e), "Deferred", "stashed, not written");
            assert!(ring.live.lock().pending.is_empty(), "not re-armed");
            assert_eq!(held.request(), &before[..], "the held slice is intact");
            assert_eq!(AnyRequest::try_from(held.request()).unwrap().unique().0, 11);
            drop(held);
            assert_eq!(state_name(e), "Pending");
            assert_eq!(ring.live.lock().pending, [0]);
            let (len, error, unique, _) = reply_fields(e);
            assert_eq!((len, error, unique), (16, -libc::ENOENT, 11));
            ring.live.lock().in_kernel = 0;
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a held fetch deadlocked or panicked");
    }

    /// CONSTELLATION PATCH (io-uring): a held fetch with no reply object answers itself with
    /// an empty reply when dropped, as an unheld one does when its dispatch returns; one whose
    /// reply object outlives it waits for that reply, which then commits directly.
    #[test]
    fn a_held_fetch_without_a_reply_is_answered_and_one_with_waits() {
        let ring = fake_ring(2, true);
        ring.ring_thread.set(thread::current().id()).ok();
        for idx in 0..2 {
            fake_fetch(
                &ring.entries[idx],
                20 + idx as u64,
                fuse_opcode::FUSE_FLUSH,
                &[0u8; 24],
                &[],
            );
            set_in_kernel(&ring, idx, 0);
        }
        let mut held = Vec::new();
        let mut hold = |commit: RingCommit, request: &[u8]| held.push(commit.hold(request));
        ring.handle_fetch(&ring.entries[0], &mut hold);
        ring.handle_fetch(&ring.entries[1], &mut hold);
        let second = held.pop().unwrap();
        drop(held);
        assert_eq!(state_name(&ring.entries[0]), "Pending", "answered empty");
        assert_eq!(reply_fields(&ring.entries[0]).1, 0);
        second.commit().reply_created();
        let commit = second.commit().clone();
        drop(second);
        assert_eq!(state_name(&ring.entries[1]), "Dispatched");
        thread::spawn(move || commit.commit_errno(Errno::EIO))
            .join()
            .unwrap();
        assert_eq!(state_name(&ring.entries[1]), "Pending");
        assert_eq!(reply_fields(&ring.entries[1]).1, -libc::EIO);
        ring.live.lock().in_kernel = 0;
    }

    /// CONSTELLATION PATCH (io-uring): a ring that leaves while a fetch is held (the session
    /// shut down) leaves the held dispatch nothing to answer: no panic, no write.
    #[test]
    fn a_held_fetch_outlives_its_ring_thread() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        fake_fetch(e, 30, fuse_opcode::FUSE_FSYNC, &[0u8; 16], &[]);
        set_in_kernel(&ring, 0, 0);
        let mut held = None;
        ring.handle_fetch(e, &mut |c: RingCommit, r: &[u8]| held = Some(c.hold(r)));
        let held = held.unwrap();
        ring.live.lock().exited = true;
        held.commit().commit_errno(Errno::EIO);
        assert_eq!(state_name(e), "Dead");
        drop(held);
        assert_eq!(state_name(e), "Dead");
        assert!(ring.live.lock().pending.is_empty());
    }

    /// CONSTELLATION PATCH (io-uring): plan 38 §3(b)'s write side. A `FUSE_WRITE` fetched over
    /// a ring reaches the filesystem as a slice of the entry's own payload buffer -- the bytes
    /// the kernel copied in, not a copy of them -- whether the ring thread dispatches it or an
    /// offload thread does through a `HeldRequest`.
    #[test]
    fn a_ring_write_borrows_the_entry_payload() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        let payload_at = e.base.0.as_ptr() as usize + e.gap;
        let mut write_in = [0u8; 40];
        write_in[16..20].copy_from_slice(&5u32.to_ne_bytes());
        for hold in [false, true] {
            fake_fetch(e, 40, fuse_opcode::FUSE_WRITE, &write_in, b"world");
            set_in_kernel(&ring, 0, 0);
            let mut held = None;
            let mut seen = None;
            ring.handle_fetch(e, &mut |c: RingCommit, request: &[u8]| {
                if hold {
                    held = Some(c.hold(request));
                    return;
                }
                let req = AnyRequest::try_from(request).unwrap();
                let Ok(Operation::Write(w)) = req.operation() else {
                    panic!("not a write")
                };
                seen = Some((w.data().as_ptr() as usize, w.data().to_vec()));
            });
            if let Some(held) = held {
                let req = AnyRequest::try_from(held.request()).unwrap();
                let Ok(Operation::Write(w)) = req.operation() else {
                    panic!("not a write")
                };
                seen = Some((w.data().as_ptr() as usize, w.data().to_vec()));
            }
            assert_eq!(seen, Some((payload_at, b"world".to_vec())), "held: {hold}");
            *e.state.lock() = EntryState::Dead;
            let mut live = ring.live.lock();
            (live.in_kernel, live.outstanding) = (0, 0);
            live.pending.clear();
        }
    }

    /// CONSTELLATION PATCH (io-uring): `fill_with(.., zero: false, ..)` hands the closure the
    /// entry's bytes as they are, and `fill` zeroes them.
    #[test]
    fn an_unzeroed_fill_sees_the_entry_as_it_was() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        poison_payload(e);
        let seen = |zero: bool| {
            let commit = fake_dispatched(&ring, 0, 5);
            let mut first = 0u8;
            commit
                .fill_with(16, zero, |buf| {
                    first = buf[0];
                    Ok(0)
                })
                .unwrap();
            *e.state.lock() = EntryState::Dead;
            ring.live.lock().in_kernel -= 1;
            ring.live.lock().pending.clear();
            first
        };
        assert_ne!(seen(false), 0, "the poison is still there");
        assert_eq!(seen(true), 0);
    }

    #[test]
    fn direct_reply_and_reply_taken() {
        let ring = fake_ring(2, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let getattr_in = [0u8; 16];

        let e = &ring.entries[0];
        fake_fetch(e, 21, fuse_opcode::FUSE_GETATTR, &getattr_in, &[]);
        set_in_kernel(&ring, 0, 0);
        set_in_kernel(&ring, 1, 0);
        let mut handler = |commit: RingCommit, _: &[u8]| {
            let header = ok_header(21);
            commit.commit(&[IoSlice::new(header.as_bytes())]).unwrap();
            assert_eq!(state_name(&commit.ring.entries[0]), "Pending");
            // A second reply for the same fetch is refused
            let err = commit
                .commit(&[IoSlice::new(header.as_bytes())])
                .unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::Other);
            // A reply object for a fetch that is over changes nothing
            commit.reply_created();
            assert_eq!(state_name(&commit.ring.entries[0]), "Pending");
        };
        ring.handle_fetch(e, &mut handler);
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().pending, [0]);
        assert_eq!(ring.live.lock().in_kernel, 2);
        assert_eq!(ring.live.lock().outstanding, 0);
        // The ring thread's own commit did not touch the eventfd
        assert_eq!(ring.wake.read(), Err(nix::errno::Errno::EAGAIN));

        let e = &ring.entries[1];
        fake_fetch(e, 22, fuse_opcode::FUSE_GETATTR, &getattr_in, &[]);
        let (tx, rx) = mpsc::channel();
        let mut handler = |commit: RingCommit, _: &[u8]| {
            // A stale handle for an earlier fetch of this entry must not flip reply_taken
            RingCommit {
                ring: Arc::clone(&commit.ring),
                idx: commit.idx,
                commit_id: 2,
            }
            .reply_created();
            assert!(matches!(
                *commit.ring.entries[1].state.lock(),
                EntryState::Dispatching {
                    reply_taken: false,
                    ..
                }
            ));
            commit.reply_created();
            tx.send(commit).unwrap();
        };
        ring.handle_fetch(e, &mut handler);
        assert_eq!(state_name(e), "Dispatched");
        assert_eq!(ring.live.lock().in_kernel, 1);
        assert_eq!(ring.live.lock().outstanding, 1);
        // The retained reply object commits later, directly, from a foreign thread
        let commit = rx.recv().unwrap();
        thread::spawn(move || {
            let header = abi::fuse_out_header {
                len: 16,
                error: -5,
                unique: 22,
            };
            commit.commit(&[IoSlice::new(header.as_bytes())]).unwrap();
        })
        .join()
        .unwrap();
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().pending, [0, 1]);
        assert_eq!(ring.live.lock().in_kernel, 2);
        assert_eq!(ring.live.lock().outstanding, 0);
        assert_eq!(
            ring.wake.read(),
            Ok(1),
            "a foreign commit wakes the ring thread"
        );

        // Without a reply object the ring thread answers with an empty OK
        let e = &ring.entries[0];
        fake_fetch(e, 23, fuse_opcode::FUSE_GETATTR, &getattr_in, &[]);
        *e.state.lock() = EntryState::InKernel { last: 21 };
        ring.live.lock().pending.clear();
        let mut handler = |_: RingCommit, _: &[u8]| {};
        ring.handle_fetch(e, &mut handler);
        assert_eq!(state_name(e), "Pending");
        let bytes = header_bytes(e);
        assert_eq!(
            (u32_at(&bytes, 0), u32_at(&bytes, 4), u64_at(&bytes, 8)),
            (16, 0, 23)
        );
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn committing_entry_is_left_to_its_committer() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        fake_fetch(e, 71, fuse_opcode::FUSE_GETATTR, &[0; 16], &[]);
        set_in_kernel(&ring, 0, 0);
        let mut handler = |commit: RingCommit, _: &[u8]| {
            *commit.ring.entries[0].state.lock() = EntryState::Committing;
        };
        ring.handle_fetch(e, &mut handler);
        assert_eq!(state_name(e), "Committing");
        let live = ring.live.lock();
        assert!(live.pending.is_empty());
        assert_eq!((live.in_kernel, live.outstanding), (0, 1));
        drop(live);
        // The header still holds the request, not a reply
        assert_eq!(u64_at(&header_bytes(e), 8), 71);
        ring.live.lock().outstanding = 0;
    }

    #[test]
    #[should_panic(expected = "right after dispatch")]
    fn dead_entry_after_dispatch_is_a_bug() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        fake_fetch(e, 72, fuse_opcode::FUSE_GETATTR, &[0; 16], &[]);
        set_in_kernel(&ring, 0, 0);
        let mut handler = |commit: RingCommit, _: &[u8]| {
            *commit.ring.entries[0].state.lock() = EntryState::Dead;
        };
        ring.handle_fetch(e, &mut handler);
    }

    #[test]
    fn oversized_or_malformed_reply_becomes_einval() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        let commit = fake_dispatched(&ring, 0, 31);
        let too_big = vec![1u8; e.payload_cap + 1];
        let header = abi::fuse_out_header {
            len: 16 + too_big.len() as u32,
            error: 0,
            unique: 31,
        };
        commit
            .commit(&[IoSlice::new(header.as_bytes()), IoSlice::new(&too_big)])
            .unwrap();
        let bytes = header_bytes(e);
        assert_eq!(u32_at(&bytes, 4) as i32, -libc::EINVAL);
        assert_eq!(u64_at(&bytes, 8), 31);
        assert_eq!(u32_at(&bytes, PAYLOAD_SZ_OFFSET), 0);
        assert_eq!(state_name(e), "Pending");

        // iov[0] that is not a fuse_out_header, direct
        let commit = fake_dispatched(&ring, 0, 32);
        commit.commit(&[IoSlice::new(b"short")]).unwrap();
        let bytes = header_bytes(e);
        assert_eq!(u32_at(&bytes, 4) as i32, -libc::EINVAL);
        assert_eq!(u64_at(&bytes, 8), 32);

        // The same during dispatch of a payload-bearing request: stashed as EINVAL, not as
        // sixteen bytes of header plus payload
        *e.state.lock() = EntryState::Dispatching {
            direct_ok: false,
            reply_taken: true,
            commit_id: 33,
        };
        let commit = RingCommit {
            ring: Arc::clone(&ring),
            idx: 0,
            commit_id: 33,
        };
        commit
            .commit(&[IoSlice::new(b"short"), IoSlice::new(&[0; 64])])
            .unwrap();
        match &*e.state.lock() {
            EntryState::Deferred {
                reply: Stashed::Bytes(bytes),
                commit_id: 33,
            } => {
                assert_eq!(bytes.0.len(), 16);
                assert_eq!(bytes.0, errno_header(33, Errno::EINVAL).as_bytes());
            }
            other => panic!("{other:?}"),
        }
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn malformed_fetch_is_answered_with_eio() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        // op_in_len 12 is not a multiple of 8
        fake_fetch(e, 61, fuse_opcode::FUSE_GETATTR, &[0; 12], &[]);
        set_in_kernel(&ring, 0, 0);
        let mut handler = |_: RingCommit, _: &[u8]| panic!("malformed requests are not dispatched");
        ring.handle_fetch(e, &mut handler);
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().in_kernel, 1, "stays counted");
        assert_eq!(ring.live.lock().pending, [0]);
        let bytes = header_bytes(e);
        assert_eq!(u32_at(&bytes, 4) as i32, -libc::EIO);
        assert_eq!(u64_at(&bytes, 8), 61);

        // commit_id 0 retires the entry instead
        fake_fetch(e, 0, fuse_opcode::FUSE_GETATTR, &[0; 16], &[]);
        *e.state.lock() = EntryState::InKernel { last: 0 };
        ring.handle_fetch(e, &mut handler);
        assert_eq!(state_name(e), "Dead");
        assert_eq!(ring.live.lock().in_kernel, 0);
        assert!(ring.live.lock().fatal.is_some());
    }

    #[test]
    fn late_commit_after_exit_is_dropped() {
        let Some(io) = try_ring_io(8, 16) else { return };
        let ring = fake_ring(2, true);
        let retained = RingCommit {
            ring: Arc::clone(&ring),
            idx: 0,
            commit_id: 99,
        };
        let started = start(&ring, io);
        started.registered();
        started
            .handler_tx
            .send(Box::new(|_: RingCommit, _: &[u8]| {}))
            .unwrap();
        assert!(started.thread.join().unwrap().is_err());
        assert!(ring.live.lock().exited);

        let header = ok_header(99);
        retained.commit(&[IoSlice::new(header.as_bytes())]).unwrap();
        retained.commit_errno(Errno::EIO);
        let live = ring.live.lock();
        assert!(live.pending.is_empty());
        assert_eq!((live.in_kernel, live.outstanding), (0, 0));
        drop(live);
        assert_eq!(state_name(&ring.entries[0]), "Dead");
        // The buffers were not touched
        assert!(header_bytes(&ring.entries[0]).iter().all(|b| *b == 0));
    }

    /// `live` is held, not `state`, because `live` is the lock the exit decision takes
    #[test]
    fn commit_racing_the_exit_is_never_lost() {
        let mut landed = 0;
        for round in 0..20 {
            let Some(mut io) = try_ring_io(8, 16) else {
                return;
            };
            let ring = fake_ring(1, true);
            let commit = fake_dispatched(&ring, 0, 70);
            ring.live.lock().conn_dead = true;
            io.push_or_submit(&ring.wake_sqe()).unwrap();
            let (tx, flushed) = mpsc::channel();
            *ring.hooks.flushed.lock() = Some(tx);
            let held = ring.live.lock();
            let server = {
                let ring = Arc::clone(&ring);
                thread::spawn(move || {
                    ring.ring_thread.set(thread::current().id()).ok();
                    io.enable().unwrap();
                    let mut handler = |_: RingCommit, _: &[u8]| {};
                    ring.serve(&mut io, &mut handler)
                })
            };
            let committer = thread::spawn(move || {
                let header = ok_header(70);
                commit.commit(&[IoSlice::new(header.as_bytes())])
            });
            // Both threads now block on `live`; whichever wins, the outcome is the same
            thread::sleep(Duration::from_millis(if round % 2 == 0 { 5 } else { 0 }));
            drop(held);
            committer.join().unwrap().unwrap();
            server.join().unwrap().unwrap();
            let live = ring.live.lock();
            assert!(live.exited);
            assert_eq!((live.in_kernel, live.outstanding), (0, 0), "round {round}");
            assert!(live.pending.is_empty(), "round {round}");
            drop(live);
            assert_eq!(state_name(&ring.entries[0]), "Dead", "round {round}");
            if flushed.try_recv().is_ok() {
                landed += 1;
            }
        }
        eprintln!("commit landed before the exit in {landed} of 20 rounds");
    }

    #[test]
    fn flush_pending_failures() {
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        let ring = fake_ring(2, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let fill_queue = |io: &mut RingIo| {
            let nop: squeue::Entry128 = opcode::Nop::new().build().user_data(WAKE - 1).into();
            // SAFETY: a Nop names no buffers.
            while unsafe { io.uring().submission().push(&nop) }.is_ok() {}
            assert!(io.uring().submission().is_full());
        };
        let queue = |idx: u32, commit_id: u64| {
            *ring.entries[idx as usize].state.lock() = EntryState::Pending { commit_id };
            let mut live = ring.live.lock();
            live.pending.push(idx);
            live.in_kernel += 1;
        };

        // The room-making submit fails per entry: retired, counted out, first error kept
        fill_queue(&mut io);
        queue(0, 81);
        io.hooks.fail_submit = Some(libc::EBUSY);
        ring.flush_pending(&mut io).unwrap();
        assert_eq!(state_name(&ring.entries[0]), "Dead");
        let live = ring.live.lock();
        assert_eq!(live.in_kernel, 0);
        assert!(live.pending.is_empty());
        assert_eq!(errno_of(live.fatal.as_ref().unwrap()), Some(libc::EBUSY));
        drop(live);
        assert!(
            io.uring().submission().is_full(),
            "the SQE was never pushed"
        );

        // A ring-level errno from the room-making submit ends the loop with the entry neither
        // pushed nor retired, which is why the mapping is then leaked
        queue(1, 82);
        io.hooks.fail_submit = Some(libc::EBADF);
        let err = ring.flush_pending(&mut io).unwrap_err();
        assert_eq!(errno_of(&err), Some(libc::EBADF));
        let live = ring.live.lock();
        assert_eq!(live.in_kernel, 1);
        assert!(live.pending.is_empty());
        drop(live);
        assert_eq!(last_command(&ring.entries[1]), Some(82));

        // With room in the queue no submit happens at all: the SQE waits for the next wait
        let Some(mut io) = try_ring_io(8, 16) else {
            return;
        };
        let ring = fake_ring(1, true);
        *ring.entries[0].state.lock() = EntryState::Pending { commit_id: 83 };
        ring.live.lock().pending.push(0);
        ring.live.lock().in_kernel = 1;
        io.hooks.fail_submit = Some(libc::EBUSY);
        ring.flush_pending(&mut io).unwrap();
        assert_eq!(last_command(&ring.entries[0]), Some(83));
        assert_eq!(io.uring().submission().len(), 1);
        assert_eq!(
            io.hooks.fail_submit,
            Some(libc::EBUSY),
            "no submit was attempted"
        );
        assert_eq!(ring.live.lock().in_kernel, 1);
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn drop_unmaps_only_when_nothing_is_in_the_kernel() {
        let _serial = UNMAP_CHECK.lock();
        let Some(ring) = big_ring(2, true) else {
            return;
        };
        let base = ring.mem.entry(0).as_ptr() as usize;
        ring.live.lock().in_kernel = 1;
        drop(ring);
        assert!(is_mapped(base), "leaked while pending");

        let Some(ring) = big_ring(2, true) else {
            return;
        };
        let base = ring.mem.entry(0).as_ptr() as usize;
        assert!(is_mapped(base));
        drop(ring);
        assert!(!is_mapped(base));
    }

    #[test]
    fn write_errno_and_hand_off_then_duplicate_commit_errno() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        *e.state.lock() = EntryState::Committing;
        ring.live.lock().outstanding = 1;
        let commit = RingCommit {
            ring: Arc::clone(&ring),
            idx: 0,
            commit_id: 91,
        };
        commit.write_errno_and_hand_off(Errno::EIO);
        let bytes = header_bytes(e);
        assert_eq!(u32_at(&bytes, 0), 16);
        assert_eq!(u32_at(&bytes, 4) as i32, -libc::EIO);
        assert_eq!(u64_at(&bytes, 8), 91);
        assert_eq!(u32_at(&bytes, PAYLOAD_SZ_OFFSET), 0);
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().pending, [0]);
        let live = ring.live.lock();
        assert_eq!((live.in_kernel, live.outstanding), (1, 0));
        drop(live);

        // A second errno reply is refused without touching anything
        commit.commit_errno(Errno::ENOENT);
        assert_eq!(header_bytes(e), bytes);
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().pending, [0]);
        assert_eq!(ring.live.lock().in_kernel, 1);
        let header = ok_header(91);
        let err = commit
            .commit(&[IoSlice::new(header.as_bytes())])
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(err.to_string(), "duplicate reply");
        // Once the connection is gone the same refusal is a late reply
        ring.live.lock().conn_dead = true;
        let err = commit
            .commit(&[IoSlice::new(header.as_bytes())])
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        ring.live.lock().conn_dead = false;

        // commit_errno from Dispatched writes the header
        *e.state.lock() = EntryState::Dispatched { commit_id: 91 };
        ring.live.lock().outstanding = 1;
        ring.live.lock().pending.clear();
        commit.commit_errno(Errno::ENOENT);
        assert_eq!(u32_at(&header_bytes(e), 4) as i32, -libc::ENOENT);
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().in_kernel, 2);
        // A stale handle for an earlier fetch of the same entry is refused too
        *e.state.lock() = EntryState::Dispatched { commit_id: 92 };
        commit.commit_errno(Errno::ENOENT);
        assert_eq!(state_name(e), "Dispatched");
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
    }

    fn payload_bytes(e: &RingEntry, len: usize) -> &[u8] {
        // SAFETY: test-owned entry with no command pending.
        unsafe { slice::from_raw_parts(e.base.0.as_ptr().add(e.gap), len) }
    }

    fn reply_fields(e: &RingEntry) -> (u32, i32, u64, u32) {
        let bytes = header_bytes(e);
        (
            u32_at(&bytes, 0),
            u32_at(&bytes, 4) as i32,
            u64_at(&bytes, 8),
            u32_at(&bytes, PAYLOAD_SZ_OFFSET),
        )
    }

    fn payload_addr(e: &RingEntry) -> usize {
        e.base.0.as_ptr().wrapping_add(e.gap) as usize
    }

    /// Poisons the first payload bytes, as an earlier request or reply would leave them
    fn poison_payload(e: &RingEntry) {
        // SAFETY: test-owned entry, no command pending.
        unsafe { ptr::write_bytes(e.base.0.as_ptr().add(e.gap), 0xEE, 16) };
    }

    /// CONSTELLATION PATCH (io-uring): `ReplyData::gather` over a ring is
    /// `fill` with the segments copied into the entry's payload in order --
    /// so it is one copy into the kernel-visible buffer and no syscall,
    /// which is the whole point of the gather form.
    #[test]
    fn gather_writes_every_segment_into_the_payload() {
        use crate::reply::Reply;
        use crate::reply::ReplyData;
        use crate::reply::ReplySender;

        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        poison_payload(e);
        let commit = fake_dispatched(&ring, 0, 71);
        let addr = payload_addr(e);
        let reply: ReplyData = Reply::new(crate::ll::RequestId(71), ReplySender::Ring(commit));
        reply.gather(&[b"hel".as_slice(), b"", b"lo wor", b"ld"]);
        assert_eq!(reply_fields(e), (16 + 11, 0, 71, 11));
        assert_eq!(payload_bytes(e, 11), b"hello world");
        assert_eq!(payload_addr(e), addr, "written in place");
        assert_eq!(ring.direct_fills(), 1, "no heap buffer, no writev");
        assert_eq!(state_name(e), "Pending");

        // No segments at all is a valid empty reply
        let commit = fake_dispatched(&ring, 0, 72);
        let reply: ReplyData = Reply::new(crate::ll::RequestId(72), ReplySender::Ring(commit));
        reply.gather::<&[u8]>(&[]);
        assert_eq!(reply_fields(e), (16, 0, 72, 0));
        assert_eq!(ring.direct_fills(), 2);
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn fill_writes_the_payload_in_place() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        poison_payload(e);
        let commit = fake_dispatched(&ring, 0, 41);
        let addr = payload_addr(e);
        let handed_back = commit
            .fill_with(8, true, |buf| {
                assert_eq!(buf.len(), 8);
                assert_eq!(buf, [0; 8]);
                assert_eq!(
                    buf.as_ptr() as usize,
                    addr,
                    "the buffer is the entry's payload"
                );
                buf[..5].copy_from_slice(b"hello");
                Ok(5)
            })
            .unwrap();
        assert!(handed_back.is_none());
        assert_eq!(reply_fields(e), (16 + 5, 0, 41, 5));
        assert_eq!(payload_bytes(e, 5), b"hello");
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().pending, [0]);
        assert_eq!(ring.live.lock().outstanding, 0);
        assert_eq!(ring.direct_fills(), 1);

        // Under-reporting the writes leaves zeros, never stale bytes, in the reply
        poison_payload(e);
        let commit = fake_dispatched(&ring, 0, 42);
        commit
            .fill_with(8, true, |buf| {
                buf[..3].copy_from_slice(b"abc");
                Ok(8)
            })
            .unwrap();
        assert_eq!(reply_fields(e), (16 + 8, 0, 42, 8));
        assert_eq!(payload_bytes(e, 8), b"abc\0\0\0\0\0");

        // An empty reply is a valid fill
        let commit = fake_dispatched(&ring, 0, 43);
        commit.fill(0, |buf| Ok(buf.len())).unwrap();
        assert_eq!(reply_fields(e), (16, 0, 43, 0));
        assert_eq!(ring.direct_fills(), 3);
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn fill_refusals_are_header_only() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];

        let commit = fake_dispatched(&ring, 0, 44);
        commit.fill(8, |_| Err(Errno::ENOENT)).unwrap();
        assert_eq!(reply_fields(e), (16, -libc::ENOENT, 44, 0), "errno row");
        assert_eq!(state_name(e), "Pending", "errno row");

        let commit = fake_dispatched(&ring, 0, 45);
        commit
            .fill(8, |buf| {
                buf.fill(0xAA);
                Ok(9)
            })
            .unwrap();
        assert_eq!(
            reply_fields(e),
            (16, -libc::EINVAL, 45, 0),
            "over-count row"
        );

        let commit = fake_dispatched(&ring, 0, 46);
        let handed_back = commit
            .fill(e.payload_cap + 1, |_| {
                unreachable!("the buffer was never made")
            })
            .unwrap();
        assert!(handed_back.is_none(), "over-capacity row");
        assert_eq!(
            reply_fields(e),
            (16, -libc::EINVAL, 46, 0),
            "over-capacity row"
        );
        assert_eq!(state_name(e), "Pending", "over-capacity row");

        // The whole capacity is fine
        let commit = fake_dispatched(&ring, 0, 47);
        commit.fill(e.payload_cap, |buf| Ok(buf.len())).unwrap();
        assert_eq!(
            reply_fields(e),
            (16 + e.payload_cap as u32, 0, 47, e.payload_cap as u32),
            "exact-capacity row"
        );

        // A refused fill never runs the closure
        *e.state.lock() = EntryState::Pending { commit_id: 47 };
        let err = commit
            .fill(8, |_| unreachable!("refused before the closure"))
            .err()
            .expect("refused");
        assert_eq!(err.to_string(), "duplicate reply");
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn fill_during_dispatch_without_a_payload_is_direct() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        fake_fetch(e, 48, fuse_opcode::FUSE_GETATTR, &[0; 16], &[]);
        set_in_kernel(&ring, 0, 0);
        let addr = payload_addr(e);
        let mut handler = |commit: RingCommit, request: &[u8]| {
            assert!(matches!(
                *commit.ring.entries[0].state.lock(),
                EntryState::Dispatching {
                    direct_ok: true,
                    ..
                }
            ));
            let handed_back = commit
                .fill(8, |buf| {
                    assert_eq!(buf.as_ptr() as usize, addr, "written in place");
                    buf[..4].copy_from_slice(b"stat");
                    Ok(4)
                })
                .unwrap();
            assert!(handed_back.is_none());
            assert_eq!(state_name(&commit.ring.entries[0]), "Pending");
            // The request slice ends before the payload area, so it is untouched
            assert_eq!(request.len(), 40 + 16);
            assert_eq!(&request[40..], &[0; 16]);
        };
        ring.handle_fetch(e, &mut handler);
        assert_eq!(state_name(e), "Pending");
        assert_eq!(reply_fields(e), (16 + 4, 0, 48, 4));
        assert_eq!(payload_bytes(e, 4), b"stat");
        assert_eq!(ring.direct_fills(), 1);
        let live = ring.live.lock();
        assert_eq!((live.in_kernel, live.outstanding), (1, 0));
        assert_eq!(live.pending, [0]);
        drop(live);
        assert_eq!(ring.wake.read(), Err(nix::errno::Errno::EAGAIN));
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn fill_during_dispatch_with_a_borrowed_payload_is_handed_back() {
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let ring = fake_ring(1, true);
            ring.ring_thread.set(thread::current().id()).ok();
            let e = &ring.entries[0];
            fake_fetch(e, 51, fuse_opcode::FUSE_LOOKUP, &[], b"hello\0");
            set_in_kernel(&ring, 0, 0);
            let mut handler = |commit: RingCommit, request: &[u8]| {
                let f = commit
                    .fill(8, |_| unreachable!("not run while the payload is borrowed"))
                    .unwrap()
                    .expect("handed back");
                assert_eq!(state_name(&commit.ring.entries[0]), "Dispatching");
                assert_eq!(&request[40..], b"hello\0");
                let _ = f;
                assert_eq!(ring.direct_fills(), 0);
                let header = abi::fuse_out_header {
                    len: 16 + 4,
                    error: 0,
                    unique: 51,
                };
                commit
                    .commit(&[IoSlice::new(header.as_bytes()), IoSlice::new(b"abcd")])
                    .unwrap();
                assert_eq!(state_name(&commit.ring.entries[0]), "Deferred");
                assert_eq!(&request[40..], b"hello\0");
                // A stale handle for an earlier fetch is refused before its closure runs
                let stale = RingCommit {
                    ring: Arc::clone(&commit.ring),
                    idx: 0,
                    commit_id: 50,
                };
                let err = stale
                    .fill(8, |_| unreachable!("refused before the closure"))
                    .err()
                    .expect("refused");
                assert_eq!(err.to_string(), "duplicate reply");
            };
            ring.handle_fetch(e, &mut handler);
            assert_eq!(state_name(e), "Pending");
            assert_eq!(reply_fields(e), (16 + 4, 0, 51, 4));
            assert_eq!(payload_bytes(e, 4), b"abcd");
            let live = ring.live.lock();
            assert_eq!((live.in_kernel, live.outstanding), (1, 0));
            assert_eq!(live.pending, [0]);
            drop(live);
            ring.live.lock().in_kernel = 0;
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handle_fetch deadlocked or panicked");
    }

    /// Nothing is written into the entry until dispatch returns, for data and for the guard's EIO
    #[test]
    fn reply_data_fill_on_a_borrowed_payload_defers_data_and_panics_alike() {
        use crate::reply::Reply;
        use crate::reply::ReplyData;
        use crate::reply::ReplySender;

        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let ring = fake_ring(1, true);
            ring.ring_thread.set(thread::current().id()).ok();
            let e = &ring.entries[0];
            fake_fetch(e, 52, fuse_opcode::FUSE_LOOKUP, &[], b"hello\0");
            set_in_kernel(&ring, 0, 0);
            let addr = payload_addr(e);
            let mut handler = |commit: RingCommit, request: &[u8]| {
                let reply: ReplyData =
                    Reply::new(crate::ll::RequestId(52), ReplySender::Ring(commit.clone()));
                reply.fill(8, |buf| {
                    assert_eq!(buf, [0; 8]);
                    assert_ne!(buf.as_ptr() as usize, addr, "a heap buffer while borrowed");
                    buf[..4].copy_from_slice(b"abcd");
                    Ok(4)
                });
                assert_eq!(state_name(&commit.ring.entries[0]), "Deferred");
                assert_eq!(&request[40..], b"hello\0");
            };
            ring.handle_fetch(e, &mut handler);
            assert_eq!(state_name(e), "Pending");
            assert_eq!(reply_fields(e), (16 + 4, 0, 52, 4));
            assert_eq!(payload_bytes(e, 4), b"abcd");

            fake_fetch(e, 53, fuse_opcode::FUSE_LOOKUP, &[], b"other\0");
            *e.state.lock() = EntryState::InKernel { last: 52 };
            ring.live.lock().pending.clear();
            let mut handler = |commit: RingCommit, request: &[u8]| {
                let reply: ReplyData =
                    Reply::new(crate::ll::RequestId(53), ReplySender::Ring(commit.clone()));
                let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reply.fill(8, |_| -> Result<usize, Errno> {
                        panic!("deliberate panic in a fill closure")
                    })
                }));
                assert!(unwound.is_err());
                assert_eq!(state_name(&commit.ring.entries[0]), "Deferred");
                assert_eq!(&request[40..], b"other\0");
                // The guard's EIO is the one reply; a second one is refused
                let err = commit.fill(8, |_| Ok(0)).err().expect("refused");
                assert_eq!(err.to_string(), "duplicate reply");
            };
            ring.handle_fetch(e, &mut handler);
            assert_eq!(state_name(e), "Pending");
            assert_eq!(reply_fields(e), (16, -libc::EIO, 53, 0));
            let live = ring.live.lock();
            assert_eq!((live.in_kernel, live.outstanding), (1, 0));
            assert_eq!(live.pending, [0]);
            drop(live);
            assert_eq!(ring.direct_fills(), 0);
            ring.live.lock().in_kernel = 0;
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handle_fetch deadlocked or panicked");
    }

    #[test]
    fn fill_panic_answers_eio_and_rearms_the_entry() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        let commit = fake_dispatched(&ring, 0, 61);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            commit.fill(8, |buf| -> Result<usize, Errno> {
                buf.fill(0xAB);
                panic!("deliberate panic in a fill closure")
            })
        }));
        assert!(unwound.is_err());
        assert_eq!(reply_fields(e), (16, -libc::EIO, 61, 0));
        assert_eq!(state_name(e), "Pending");
        let live = ring.live.lock();
        assert_eq!((live.in_kernel, live.outstanding), (1, 0));
        assert_eq!(live.pending, [0]);
        drop(live);
        let header = header_bytes(e);
        commit.commit_errno(Errno::EIO);
        assert_eq!(header_bytes(e), header);
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().pending, [0]);
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn fill_from_a_foreign_thread_wakes_the_ring() {
        let ring = fake_ring(1, true);
        ring.ring_thread.set(thread::current().id()).ok();
        let e = &ring.entries[0];
        let commit = fake_dispatched(&ring, 0, 71);
        thread::spawn(move || {
            commit
                .fill(4, |buf| {
                    buf.copy_from_slice(b"wxyz");
                    Ok(4)
                })
                .unwrap();
        })
        .join()
        .unwrap();
        assert_eq!(reply_fields(e), (16 + 4, 0, 71, 4));
        assert_eq!(payload_bytes(e, 4), b"wxyz");
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.wake.read(), Ok(1));

        let commit = fake_dispatched(&ring, 0, 72);
        let unwound = thread::spawn(move || {
            commit.fill(4, |_| -> Result<usize, Errno> {
                panic!("deliberate panic in a fill closure")
            })
        })
        .join();
        assert!(unwound.is_err());
        assert_eq!(reply_fields(e), (16, -libc::EIO, 72, 0));
        assert_eq!(state_name(e), "Pending");
        assert_eq!(ring.live.lock().pending, [0, 0]);
        assert_eq!(ring.wake.read(), Ok(1), "the guard woke the ring thread");
        *e.state.lock() = EntryState::Dead;
        ring.live.lock().in_kernel = 0;
    }

    #[test]
    fn debug_output_names_states_without_payloads() {
        let ring = fake_ring(1, true);
        let commit = RingCommit {
            ring,
            idx: 0,
            commit_id: 5,
        };
        assert_eq!(
            format!("{commit:?}"),
            "RingCommit { ring: 7, idx: 0, commit_id: 5 }"
        );
        let deferred = EntryState::Deferred {
            reply: Stashed::Bytes(ReplyBytes(vec![0xAB; 4096])),
            commit_id: 8,
        };
        assert_eq!(
            format!("{deferred:?}"),
            "Deferred { reply: Bytes(4096 bytes), commit_id: 8 }"
        );
        assert_eq!(
            format!("{:?}", EntryState::InKernel { last: 0 }),
            "InKernel { last: 0 }"
        );
    }
}
