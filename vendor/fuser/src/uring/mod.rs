//! CONSTELLATION PATCH (io-uring): this whole file is Constellation's, not upstream
//! fuser's (vendor/fuser/CONSTELLATION-PATCH.md, patches/0002-io-uring-transport.patch).
//!
//! FUSE-over-io_uring transport (kernel protocol 7.42 and later).
//!
//! Userspace registers per-CPU queues of entries with `IORING_OP_URING_CMD` SQEs on an SQE128
//! ring. The kernel fills an entry with a request and posts a CQE; the reply is written into the
//! same entry and committed with `COMMIT_AND_FETCH`, which re-arms the entry. Every SQE of a ring
//! is submitted by that ring's own thread, because the kernel delivers the next request into an
//! entry as task work of the task that submitted the entry's SQE; the ring is created
//! `SINGLE_ISSUER | DEFER_TASKRUN` so that task work runs inside that thread's own
//! `io_uring_enter` rather than being signalled to it.

pub(crate) mod mem;
pub(crate) mod memory;
pub(crate) mod ring;
pub(crate) mod staging;

use std::fmt;
use std::fs;
use std::io;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;

use log::debug;
use log::warn;
use nix::unistd::SysconfVar;

use crate::dev_fuse::DevFuse;
use crate::session::spawn_named;
use crate::uring::mem::PoolMemory;
use crate::uring::ring::FetchHandler;
use crate::uring::ring::IORING_MAX_ENTRIES;
use crate::uring::ring::PoolSetup;
use crate::uring::ring::Ring;
use crate::uring::ring::RingIo;
use crate::uring::ring::SetupGate;
use crate::uring::ring::TableSetup;
use crate::uring::ring::ring_sizes;

/// The kernel's `cpu_possible_mask`, which is the number of queues `REGISTER` must populate
/// before the connection becomes ready.
const POSSIBLE_CPUS: &str = "/sys/devices/system/cpu/possible";

/// The fuse module's knob: `Y` only on 6.14+ booted with `fuse.enable_uring=1`. The kernel
/// advertises `FUSE_OVER_IO_URING` in `FUSE_INIT` exactly when this is `Y`.
const ENABLE_URING: &str = "/sys/module/fuse/parameters/enable_uring";

/// CONSTELLATION PATCH (io-uring): the most pages a FUSE request may carry
/// (`fs.fuse.max_pages_limit`, default 256), to which the kernel clamps a connection's
/// `max_pages` when it is created.
const MAX_PAGES_LIMIT: &str = "/proc/sys/fs/fuse/max_pages_limit";

/// CONSTELLATION PATCH (io-uring): `fs.fuse.max_pages_limit` (`MAX_PAGES_LIMIT`).
pub(crate) fn max_pages_limit() -> io::Result<usize> {
    let text = fs::read_to_string(MAX_PAGES_LIMIT)?;
    text.trim()
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{MAX_PAGES_LIMIT} reads {:?}", text.trim()),
            )
        })
}

/// CONSTELLATION PATCH (io-uring): whether the effective capability set holds
/// `CAP_SYS_ADMIN`, which the kernel's `ADD_QUEUE` demands of a zero-copy queue (plan 38 Z4).
/// Not proof the kernel's `capable()` agrees -- in a user namespace it does not -- which is why
/// a refused zero-copy queue degrades the session instead of failing it (`Ring::set_up_queues`).
pub(crate) fn has_cap_sys_admin() -> bool {
    const CAP_SYS_ADMIN: u32 = 21;
    const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;
    #[repr(C)]
    struct Header {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Data {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    let mut header = Header {
        version: LINUX_CAPABILITY_VERSION_3,
        pid: 0,
    };
    let mut data = [Data::default(); 2];
    // SAFETY: `capget(2)` with a version 3 header writes two `Data` structs, which `data` is.
    let res = unsafe {
        libc::syscall(
            libc::SYS_capget,
            &mut header as *mut Header,
            data.as_mut_ptr(),
        )
    };
    res == 0 && data[0].effective & (1 << CAP_SYS_ADMIN) != 0
}

/// Every ring of a session and the thread serving each.
///
/// Created before the INIT reply with the threads parked, so that any failure here leaves the
/// session free to use `/dev/fuse` instead. `start` registers the queues once the reply that
/// committed the kernel to them was written; the session hands each thread its handler when
/// it runs. Dropping the set detaches the threads, whose exit depends on the kernel ending
/// the connection.
pub(crate) struct RingSet {
    rings: Vec<Arc<Ring>>,
    threads: Vec<JoinHandle<io::Result<()>>>,
    go: Vec<mpsc::Sender<()>>,
    registered: Vec<mpsc::Receiver<io::Result<()>>>,
    handler_tx: Vec<mpsc::Sender<Box<dyn FetchHandler>>>,
    /// CONSTELLATION PATCH (io-uring): each ring's buffer table (`Ring::setup_gate`).
    tables: Vec<mpsc::Receiver<io::Result<TableSetup>>>,
    /// CONSTELLATION PATCH (io-uring): each ring's go-ahead for its zero-copy queues, once all
    /// tables are in place (`true`), or to withdraw its table (`false`).
    queues: Vec<mpsc::Sender<bool>>,
    /// CONSTELLATION PATCH (io-uring): each ring's pool setup (`Ring::setup_gate`).
    setup: Vec<mpsc::Receiver<io::Result<PoolSetup>>>,
    /// CONSTELLATION PATCH (io-uring): each ring's go-ahead to register, once all set up.
    proceed: Vec<mpsc::Sender<()>>,
    /// CONSTELLATION PATCH (io-uring): zero-copy was asked for (`ZeroCopyPlan`).
    zero_copy_asked: bool,
    /// CONSTELLATION PATCH (io-uring): every queue of every ring is a zero-copy queue; known
    /// once `start` returned.
    zero_copy: bool,
}

/// CONSTELLATION PATCH (io-uring): what the handshake grants a session that may use zero-copy
/// queues (plan 38 Z4, `Session::zero_copy_plan`): the size of a pool buffer, which must be the
/// kernel's `max_payload_sz` for the negotiated INIT exactly, and whether the pools are
/// registered as an io_uring fixed buffer (pinned) or handed over as plain memory.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ZeroCopyPlan {
    pub(crate) buf_size: usize,
    pub(crate) register_pool: bool,
}

impl fmt::Debug for RingSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingSet")
            .field("rings", &self.rings.len())
            .finish_non_exhaustive()
    }
}

/// CONSTELLATION PATCH (io-uring): a hook called once each time a queue's lock-wait budget
/// serves a blocking lock request (`FUSE_SETLKW`) as a non-blocking one
/// (`HeldRequest::downgrade_lock_wait`, `RingCommit::reserve_lock_wait`): a conflict on such a
/// request is answered `ENOLCK` instead of waiting. Called on the ring thread, before the
/// request is dispatched, so it must not block. Two hooks are equal when they are the same
/// one (`Config` is `Eq`).
#[derive(Clone)]
pub struct LockWaitDowngrades(Arc<dyn Fn() + Send + Sync>);

impl LockWaitDowngrades {
    /// A hook calling `f`.
    pub fn new(f: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    pub(crate) fn notify(&self) {
        (self.0)()
    }
}

impl fmt::Debug for LockWaitDowngrades {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LockWaitDowngrades")
    }
}

impl PartialEq for LockWaitDowngrades {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for LockWaitDowngrades {}

/// CONSTELLATION PATCH (io-uring): what a ring session reports about requests it holds, shared
/// by all of its rings and read by the filesystem (`Config::io_uring_health`).
///
/// - `stranded_commits`: replies another thread handed to a ring thread whose wake-up never
///   came. The ring thread found each still queued after an idle wait timed out, and flushed it
///   then; without that bounded wait the caller in the kernel would have waited until the next
///   request on one of that ring's queues woke the thread. Non-zero means a lost wake-up, in
///   this crate or the kernel: worth reporting, and no longer a hang.
/// - `entries_held_long`: ring entries userspace has held -- fetched, not yet committed back
///   -- for at least `held_report_after`, as of the last watchdog pass (every quarter of that,
///   at least once a second), blocking lock requests excepted. Each is a caller the kernel
///   holds while the filesystem may not even have seen its request (it can wait for an
///   offload thread), so a filesystem's own count of requests in flight does not show them.
///   Each newly long-held entry is also logged once, with a per-queue census.
#[derive(Clone)]
pub struct RingHealth(Arc<RingHealthInner>);

struct RingHealthInner {
    held_report_after: std::time::Duration,
    stranded_commits: std::sync::atomic::AtomicU64,
    entries_held_long: std::sync::atomic::AtomicU64,
}

impl RingHealth {
    /// Reports entries held in userspace for `held_report_after` or longer; zero runs no
    /// watchdog (`entries_held_long` stays 0) and still counts stranded commits.
    pub fn new(held_report_after: std::time::Duration) -> Self {
        Self(Arc::new(RingHealthInner {
            held_report_after,
            stranded_commits: Default::default(),
            entries_held_long: Default::default(),
        }))
    }

    /// Replies flushed only by a ring thread's idle wait, so far.
    pub fn stranded_commits(&self) -> u64 {
        self.0
            .stranded_commits
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Entries held in userspace past `held_report_after` at the last watchdog pass.
    pub fn entries_held_long(&self) -> u64 {
        self.0
            .entries_held_long
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn held_report_after(&self) -> std::time::Duration {
        self.0.held_report_after
    }

    /// Counts `n` more stranded commits; the new total.
    pub(crate) fn note_stranded(&self, n: u64) -> u64 {
        self.0
            .stranded_commits
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed)
            + n
    }

    pub(crate) fn set_entries_held_long(&self, n: u64) {
        self.0
            .entries_held_long
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }
}

impl fmt::Debug for RingHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingHealth")
            .field("held_report_after", &self.0.held_report_after)
            .field("stranded_commits", &self.stranded_commits())
            .field("entries_held_long", &self.entries_held_long())
            .finish()
    }
}

impl PartialEq for RingHealth {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for RingHealth {}

impl RingSet {
    /// CONSTELLATION PATCH (io-uring): tells `hook` about every lock-wait budget downgrade on
    /// any of this set's rings (`LockWaitDowngrades`).
    pub(crate) fn set_lock_wait_downgrades(&self, hook: &LockWaitDowngrades) {
        for ring in &self.rings {
            ring.set_lock_wait_downgrades(hook.clone());
        }
    }

    /// CONSTELLATION PATCH (io-uring): every ring of this set counts into `health`
    /// (`RingHealth`).
    pub(crate) fn set_health(&self, health: &RingHealth) {
        for ring in &self.rings {
            ring.set_health(health.clone());
        }
    }

    /// CONSTELLATION PATCH (io-uring): starts the thread that reports entries userspace holds
    /// past `health`'s threshold (`Ring::report_held`, `RingHealth::entries_held_long`). It
    /// holds the rings weakly and leaves once they are all gone.
    pub(crate) fn watch_held(&self, health: RingHealth) -> io::Result<()> {
        let rings: Vec<std::sync::Weak<Ring>> = self.rings.iter().map(Arc::downgrade).collect();
        let after = health.held_report_after();
        let tick = (after / 4).max(std::time::Duration::from_secs(1));
        let after_ms = after.as_millis() as u64;
        thread::Builder::new()
            .name("fuser-ring-watch".into())
            .spawn(move || loop {
                thread::sleep(tick);
                let mut alive = false;
                let mut long = 0;
                for ring in rings.iter().filter_map(std::sync::Weak::upgrade) {
                    alive = true;
                    long += ring.report_held(after_ms);
                }
                if !alive {
                    return;
                }
                health.set_entries_held_long(long);
            })
            .map(drop)
    }

    /// Opens the io_urings of `min(n_threads, queues)` rings over every possible CPU's queue,
    /// reserves their buffers and spawns their parked threads. The error text names what
    /// failed, for the session's fallback warning.
    ///
    /// CONSTELLATION PATCH (io-uring): `backend` serves the rings from an `InMemoryRingKernel`
    /// instead of `io_uring_setup(2)`; `malformed_register` makes every REGISTER one the kernel
    /// refuses (`Ring::set_malformed_register`), for the fault-injection tests of the fallback.
    /// `zero_copy` reserves each ring's buffer pools for `start` to try zero-copy queues with,
    /// after everything else; a reservation that fails leaves the whole set without it, logged
    /// here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        device: Arc<DevFuse>,
        mounted: bool,
        n_threads: usize,
        depth: u32,
        payload_cap: usize,
        backend: Option<&memory::InMemoryRingKernel>,
        malformed_register: bool,
        zero_copy: Option<ZeroCopyPlan>,
    ) -> io::Result<Self> {
        let n_queues = possible_cpus().map_err(|err| {
            io::Error::other(format!("the possible CPU count is unknown ({err})"))
        })?;
        let rings = partition(n_queues, n_threads);
        // Beyond the largest ring the completion queue overflows and fetches can be lost
        let largest = rings.iter().map(Vec::len).max().unwrap_or(0);
        let entries = largest.saturating_mul(depth as usize);
        if entries > IORING_MAX_ENTRIES {
            return Err(io::Error::other(format!(
                "{largest} queues x depth {depth} exceed the {IORING_MAX_ENTRIES} entries an \
                 io_uring holds (lower io_uring_queue_depth or raise n_threads)"
            )));
        }
        let mut set = Self {
            rings: Vec::new(),
            threads: Vec::new(),
            go: Vec::new(),
            registered: Vec::new(),
            handler_tx: Vec::new(),
            tables: Vec::new(),
            queues: Vec::new(),
            setup: Vec::new(),
            proceed: Vec::new(),
            zero_copy_asked: false,
            zero_copy: false,
        };
        let mut bodies = Vec::new();
        let ring_queues: Vec<usize> = rings.iter().map(Vec::len).collect();
        for (index, qids) in rings.into_iter().enumerate() {
            // The cheapest and likeliest refusal (a sandbox without io_uring) comes first
            let (sq, cq) = ring_sizes(qids.len() * depth as usize);
            let io = match backend {
                Some(kernel) => RingIo::memory(kernel.ring(index)?),
                None => RingIo::open(sq, cq)
                    .map_err(|err| io::Error::other(format!("io_uring_setup failed ({err})")))?,
            };
            let ring = Ring::new(index, mounted, device.clone(), &qids, depth, payload_cap)?;
            if malformed_register {
                ring.set_malformed_register();
            }
            let (table_tx, table_rx) = mpsc::channel();
            let (queues_tx, queues_rx) = mpsc::channel();
            let (setup_tx, setup_rx) = mpsc::channel();
            let (proceed_tx, proceed_rx) = mpsc::channel();
            ring.set_setup_gate(SetupGate {
                table: table_tx,
                queues: queues_rx,
                setup: setup_tx,
                proceed: proceed_rx,
            });
            set.tables.push(table_rx);
            set.queues.push(queues_tx);
            set.setup.push(setup_rx);
            set.proceed.push(proceed_tx);
            let (go_tx, go_rx) = mpsc::channel();
            let (registered_tx, registered_rx) = mpsc::channel();
            let (handler_tx, handler_rx) = mpsc::channel();
            bodies.push((format!("fuser-ring-{index}"), {
                let ring = Arc::clone(&ring);
                move || ring.thread_main(io, go_rx, registered_tx, handler_rx)
            }));
            set.rings.push(ring);
            set.go.push(go_tx);
            set.registered.push(registered_rx);
            set.handler_tx.push(handler_tx);
        }
        set.threads = spawn_named(bodies)
            .map_err(|err| io::Error::other(format!("creating the ring threads failed ({err})")))?;
        // CONSTELLATION PATCH (io-uring): the pools last, once everything the ring itself needs
        // is in place, so that a limit on the address space (`RLIMIT_AS`) costs zero-copy
        // before it costs the ring: the optional rung gives way first. Address space only
        // (`MAP_NORESERVE`) until a ring registers its pool as a fixed buffer, which pins it.
        if let Some(plan) = zero_copy {
            let pools: io::Result<Vec<PoolMemory>> = ring_queues
                .iter()
                .map(|&queues| PoolMemory::new(queues, depth as usize, plan.buf_size))
                .collect();
            match pools {
                Ok(pools) => {
                    for (ring, pool) in set.rings.iter().zip(pools) {
                        ring.reserve_pools(pool, plan.register_pool);
                    }
                    set.zero_copy_asked = true;
                }
                Err(err) => {
                    warn!("io_uring zero-copy unavailable: {err}; serving io_uring without it");
                }
            }
        }
        debug!(
            "io_uring: {n_queues} queues over {} rings, depth {depth}, payload {payload_cap} \
             bytes per entry, {} bytes reserved, {} bytes of buffer pools",
            set.rings.len(),
            set.rings.iter().map(|r| r.reserved_bytes()).sum::<usize>(),
            set.rings.iter().map(|r| r.pool_bytes()).sum::<usize>()
        );
        Ok(set)
    }

    /// CONSTELLATION PATCH (io-uring): whether every queue is a zero-copy queue (plan 38 Z4),
    /// as `start` settled it.
    pub(crate) fn zero_copy(&self) -> bool {
        self.zero_copy
    }

    /// Releases the parked threads to register their queues and waits for every ring's
    /// REGISTER submit. Only valid once the INIT reply echoing the flag was written; an
    /// error here leaves the mount blocked on queues nobody serves, so the caller must end
    /// the session, and the rings that did register are abandoned (`Ring::abandon`).
    ///
    /// CONSTELLATION PATCH (io-uring): in three steps, each a barrier across every ring. Each
    /// ring registers its buffer table (`Ring::set_up_table`); only if every ring could does any
    /// ring create a queue, else every ring withdraws its table; then each ring sets up its
    /// queues and their pools (`Ring::set_up_queues`), and only once every ring has done so
    /// does any ring register an entry. So the likeliest refusal, a table the process may not
    /// pin, can never leave one ring zero-copy and another not; whether zero-copy happens is
    /// settled for the whole session -- one transport for every reply of it
    /// (`Ring::set_session_zero_copy`), logged once with the reason when it does not -- before
    /// the first REGISTER, after which the kernel would refuse to change a queue's mind.
    pub(crate) fn start(&mut self) -> io::Result<()> {
        for go in self.go.drain(..) {
            // A thread that is already gone shows up as a disconnected `tables` below
            let _ = go.send(());
        }
        let mut tables = Vec::new();
        let mut failed = None;
        for (index, table) in self.tables.drain(..).enumerate() {
            let result = table.recv().map_err(|_| {
                io::Error::other(format!(
                    "io_uring: ring {index} thread exited before setting up its buffer table"
                ))
            });
            match result.and_then(|r| r) {
                Ok(table) => tables.push(table),
                Err(err) => {
                    failed.get_or_insert(err);
                }
            }
        }
        if let Some(err) = failed {
            // Dropping `queues` releases the waiting rings without creating anything
            self.queues.clear();
            self.proceed.clear();
            for ring in &self.rings {
                ring.abandon();
            }
            self.handler_tx.clear();
            return Err(err);
        }
        let refused_table = tables.iter().find_map(|t| match t {
            TableSetup::Refused(why) => Some(why.clone()),
            _ => None,
        });
        let go_queues =
            self.zero_copy_asked && tables.iter().all(|t| matches!(t, TableSetup::Ready));
        for queues in self.queues.drain(..) {
            // A thread that is already gone shows up as a disconnected `setup` below
            let _ = queues.send(go_queues);
        }
        let mut setups = Vec::new();
        let mut failed = None;
        for (index, setup) in self.setup.drain(..).enumerate() {
            let result = setup.recv().map_err(|_| {
                io::Error::other(format!(
                    "io_uring: ring {index} thread exited before setting up its queues"
                ))
            });
            match result.and_then(|r| r) {
                Ok(setup) => setups.push(setup),
                Err(err) => {
                    failed.get_or_insert(err);
                }
            }
        }
        if let Some(err) = failed {
            // Dropping `proceed` releases the waiting rings without registering anything
            self.proceed.clear();
            for ring in &self.rings {
                ring.abandon();
            }
            self.handler_tx.clear();
            return Err(err);
        }
        self.zero_copy = go_queues
            && !setups.is_empty()
            && setups.iter().all(|s| {
                matches!(
                    s,
                    PoolSetup::Pools {
                        zero_copy: true,
                        ..
                    }
                )
            });
        if self.zero_copy_asked {
            // A refused table is the reason the others were withdrawn, so it comes first
            let why = refused_table.as_deref().or_else(|| {
                setups.iter().find_map(|s| match s {
                    PoolSetup::Degraded(why) => Some(why.as_str()),
                    PoolSetup::Pools { note, .. } => note.as_deref(),
                    PoolSetup::NotAsked => None,
                })
            });
            match (self.zero_copy, why) {
                (true, None) => debug!("io_uring: zero-copy on every queue"),
                (true, Some(why)) => warn!("io_uring: zero-copy on every queue, but {why}"),
                (false, why) => warn!(
                    "io_uring zero-copy unavailable: {}; serving io_uring without it",
                    why.unwrap_or("a ring did not set up its queues")
                ),
            }
        }
        for ring in &self.rings {
            ring.set_session_zero_copy(self.zero_copy);
        }
        for proceed in self.proceed.drain(..) {
            let _ = proceed.send(());
        }
        for (index, registered) in self.registered.drain(..).enumerate() {
            let result = registered.recv().map_err(|_| {
                io::Error::other(format!(
                    "io_uring: ring {index} thread exited before registering"
                ))
            });
            if let Err(err) = result.and_then(|r| r) {
                for ring in &self.rings {
                    ring.abandon();
                }
                self.handler_tx.clear();
                return Err(err);
            }
        }
        Ok(())
    }

    /// Hands every ring thread the handler `make(index)` builds for it.
    pub(crate) fn serve(
        &self,
        mut make: impl FnMut(usize) -> Box<dyn FetchHandler>,
    ) -> io::Result<()> {
        for (index, handler_tx) in self.handler_tx.iter().enumerate() {
            handler_tx.send(make(index)).map_err(|_| {
                io::Error::other(format!(
                    "io_uring: ring {index} thread exited before serving"
                ))
            })?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn rings(&self) -> &[Arc<Ring>] {
        &self.rings
    }

    /// `Ring::shutdown` for every ring; call after the connection ended.
    pub(crate) fn shutdown(&self) {
        for ring in &self.rings {
            ring.shutdown();
        }
    }

    pub(crate) fn take_threads(&mut self) -> Vec<JoinHandle<io::Result<()>>> {
        std::mem::take(&mut self.threads)
    }
}

impl Drop for RingSet {
    /// Threads still attached were never joined by `run` and are not joined now: a registered
    /// ring drains to the end of the connection, which nothing here can wait for.
    fn drop(&mut self) {
        if !self.threads.is_empty() {
            self.shutdown();
            debug!("io_uring: detaching {} ring threads", self.threads.len());
        }
    }
}

/// CONSTELLATION PATCH (io-uring): the kernel refused a ring's registration after the
/// `FUSE_INIT` reply had committed the connection to rings.
///
/// The kernel validates a REGISTER when it is issued and refuses a malformed one at once (see
/// `Ring::register_all`). Nothing can serve such a connection: the kernel does not route its
/// requests back to `/dev/fuse` (plan 38 Z0a), and holds every one of them until the ring is
/// ready. So the session's constructor fails with this, wrapped in an `io::Error`, and its drop
/// ends the connection; a caller that wants the plan 38 §2.4 ladder mounts again without the
/// ring ([`Self::is`] tells this refusal from any other failure of the constructor).
#[derive(Debug)]
pub struct RegistrationRefused {
    ring: usize,
    qid: u16,
    error: io::Error,
}

impl RegistrationRefused {
    pub(crate) fn error(ring: usize, qid: u16, error: io::Error) -> io::Error {
        io::Error::other(Self { ring, qid, error })
    }

    /// Whether `err` is a refused registration.
    pub fn is(err: &io::Error) -> bool {
        err.get_ref().is_some_and(|e| e.is::<Self>())
    }

    /// The kernel's own error.
    pub fn kernel_error(&self) -> &io::Error {
        &self.error
    }
}

impl fmt::Display for RegistrationRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the kernel refused to register io_uring queue {} of ring {} ({})",
            self.qid, self.ring, self.error
        )
    }
}

impl std::error::Error for RegistrationRefused {}

/// Number of queues the kernel expects, from `num_possible_cpus()`.
///
/// `sysconf(_SC_NPROCESSORS_CONF)` counts present CPUs, which on hotplug-capable VMs is less
/// than the possible count; registering that many queues would leave every request on the mount
/// blocked. So only sysfs is consulted, and a value below the present count means the file
/// is not what this expects. Callers fall back to `/dev/fuse` on `Err`.
pub(crate) fn possible_cpus() -> io::Result<u16> {
    let text = fs::read_to_string(POSSIBLE_CPUS)?;
    let count = possible_cpus_parse(&text)?;
    let present = nix::unistd::sysconf(SysconfVar::_NPROCESSORS_CONF)
        .ok()
        .flatten()
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    if count < present {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{POSSIBLE_CPUS} lists {count} CPUs but {present} are present"),
        ));
    }
    u16::try_from(count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{count} possible CPUs exceed the 16-bit queue id"),
        )
    })
}

/// Counts the CPUs in a sysfs CPU list such as `0-3,8-11`.
fn possible_cpus_parse(text: &str) -> io::Result<usize> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("cannot parse CPU list {text:?}"),
        )
    };
    let mut count = 0usize;
    for range in text.trim().split(',') {
        let (lo, hi) = match range.split_once('-') {
            Some((lo, hi)) => (lo, hi),
            None => (range, range),
        };
        let lo: usize = lo.parse().map_err(|_| invalid())?;
        let hi: usize = hi.parse().map_err(|_| invalid())?;
        if hi < lo {
            return Err(invalid());
        }
        count = count.checked_add(hi - lo + 1).ok_or_else(invalid)?;
    }
    if count == 0 {
        return Err(invalid());
    }
    Ok(count)
}

/// Assigns the queues `0..n_queues` round-robin to `min(n_threads, n_queues)` rings. Ring `r`
/// owns queue `q` when `q % rings == r`, so every queue belongs to exactly one ring.
pub(crate) fn partition(n_queues: u16, n_threads: usize) -> Vec<Vec<u16>> {
    let rings = n_threads.max(1).min(usize::from(n_queues));
    (0..rings)
        .map(|r| {
            (r..usize::from(n_queues))
                .step_by(rings)
                .map(|q| q as u16)
                .collect()
        })
        .collect()
}

/// Why a session asking for the ring on this host would fall back to `/dev/fuse`, or `None`
/// when the host would grant it: the fuse module's `enable_uring` knob, `/dev/fuse` itself,
/// and an actual `io_uring_setup(2)` -- the call a seccomp policy or a container runtime can
/// deny with nothing in sysfs to say so (plan 38 §8).
///
/// Best effort, for tests and diagnostics: the refusals left (the buffer reservation, the
/// kernel's REGISTER) need the negotiated sizes of a live connection. A session never
/// consults this -- it tries, and logs the real reason when it falls back.
pub fn uring_unavailable() -> Option<String> {
    match fs::read_to_string(ENABLE_URING) {
        Ok(v) if v.trim() == "Y" => {}
        Ok(v) => return Some(format!("fuse.enable_uring is {}", v.trim())),
        Err(e) => return Some(format!("fuse.enable_uring unreadable: {e}")),
    }
    if !std::path::Path::new("/dev/fuse").exists() {
        return Some("/dev/fuse is missing".into());
    }
    if let Err(e) = RingIo::open(8, 16) {
        return Some(format!("io_uring_setup failed: {e}"));
    }
    None
}

#[cfg(test)]
mod test {
    use super::*;

    /// The probe answers, and agrees with the knob it reads: a host whose
    /// `enable_uring` is not `Y` can never be granted the ring, and one where it
    /// is may still be refused the `io_uring_setup` the probe makes.
    #[test]
    fn the_probe_never_contradicts_the_kernel_knob() {
        let enabled = fs::read_to_string(ENABLE_URING).is_ok_and(|v| v.trim() == "Y");
        match uring_unavailable() {
            None => assert!(enabled, "granted the ring with enable_uring off"),
            Some(why) => assert!(!why.is_empty(), "a refusal always names a reason"),
        }
        if !enabled {
            assert!(
                uring_unavailable().is_some_and(|w| w.starts_with("fuse.enable_uring")),
                "the knob is the first thing refused"
            );
        }
    }

    #[test]
    fn parses_cpu_lists() {
        assert_eq!(possible_cpus_parse("0-447\n").unwrap(), 448);
        assert_eq!(possible_cpus_parse("0").unwrap(), 1);
        assert_eq!(possible_cpus_parse("0-3,8-11").unwrap(), 8);
        for garbage in ["", "\n", "abc", "3-1", "0-", "-1", "0,,1"] {
            let err = possible_cpus_parse(garbage).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{garbage:?}");
        }
    }

    #[test]
    fn possible_cpus_agrees_with_sysfs() {
        let count = possible_cpus().unwrap();
        let text = fs::read_to_string(POSSIBLE_CPUS).unwrap();
        assert_eq!(usize::from(count), possible_cpus_parse(&text).unwrap());
        let present = nix::unistd::sysconf(SysconfVar::_NPROCESSORS_CONF)
            .unwrap()
            .unwrap();
        assert!(i64::from(count) >= present);
    }

    #[test]
    fn oversized_depth_is_refused_up_front() {
        let device = Arc::new(DevFuse(fs::File::open("/dev/zero").unwrap()));
        let queues = usize::from(possible_cpus().unwrap());
        let depth = (IORING_MAX_ENTRIES / queues + 1) as u32;
        let err =
            RingSet::new(device.clone(), true, 1, depth, 8192, None, false, None).unwrap_err();
        assert!(
            err.to_string()
                .starts_with(&format!("{queues} queues x depth {depth} exceed")),
            "{err}"
        );
        assert!(RingSet::new(device, true, 1, u32::MAX, 8192, None, false, None).is_err());
    }

    #[test]
    fn partition_is_round_robin_and_complete() {
        let one = partition(448, 1);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].len(), 448);
        assert_eq!(one[0], (0..448).collect::<Vec<u16>>());

        let three = partition(448, 3);
        assert_eq!(
            three.iter().map(Vec::len).collect::<Vec<_>>(),
            [150, 149, 149]
        );
        let mut all: Vec<u16> = three.iter().flatten().copied().collect();
        all.sort_unstable();
        assert_eq!(all, (0..448).collect::<Vec<u16>>());
        for (r, qids) in three.iter().enumerate() {
            assert!(qids.iter().all(|&q| usize::from(q) % 3 == r));
        }

        // More threads than queues clamps to one ring per queue; zero threads means one ring
        assert_eq!(partition(4, 16), [[0], [1], [2], [3]]);
        assert_eq!(partition(4, 0), [[0, 1, 2, 3]]);
        assert!(partition(0, 3).is_empty());
    }

    /// CONSTELLATION PATCH (io-uring): upstream 0.18.0 has no test module in
    /// `lib.rs`, so the fork's `enable_io_uring_is_echoed_only_when_offered`
    /// lives here, where the transport it is about does.
    #[test]
    fn enable_io_uring_is_echoed_only_when_offered() {
        use zerocopy::IntoBytes;

        use crate::InitFlags;
        use crate::KernelConfig;
        use crate::ll;
        use crate::ll::fuse_abi::fuse_in_header;
        use crate::ll::fuse_abi::fuse_init_in;
        use crate::ll::fuse_abi::fuse_opcode;
        use crate::uring::staging::test::in_header;

        let init_request = |flags: InitFlags| -> Vec<u8> {
            let (flags_lo, flags_hi) = (flags | InitFlags::FUSE_INIT_EXT).pair();
            let len = (size_of::<fuse_in_header>() + size_of::<fuse_init_in>()) as u32;
            let header = in_header(len, fuse_opcode::FUSE_INIT as u32, 1);
            let arg = fuse_init_in {
                major: 7,
                minor: 41,
                max_readahead: 65536,
                flags: flags_lo,
                flags2: flags_hi,
                unused: [0; 11],
            };
            [&header[..], arg.as_bytes()].concat()
        };
        let flags2_of = |request: &[u8], enable: bool| -> u32 {
            let request = ll::AnyRequest::try_from(request).unwrap();
            let ll::Operation::Init(init) = request.operation().unwrap() else {
                panic!("not an init request");
            };
            let mut config =
                KernelConfig::new(init.capabilities(), init.max_readahead(), init.version());
            assert_eq!(
                config.add_capabilities(InitFlags::FUSE_OVER_IO_URING),
                Err(InitFlags::FUSE_OVER_IO_URING),
                "only the session may request the bit"
            );
            if enable {
                config.enable_io_uring();
            }
            let response = init.reply(&config);
            ll::reply::Response::with_iovec(&response, request.unique(), |iov| {
                let bytes: Vec<u8> = iov.iter().flat_map(|s| s.iter().copied()).collect();
                // fuse_out_header (16) then fuse_init_out; flags2 is at offset 32 of the latter
                u32::from_ne_bytes(bytes[16 + 32..16 + 36].try_into().unwrap())
            })
        };
        let (_, io_uring_hi) = InitFlags::FUSE_OVER_IO_URING.pair();
        assert_eq!(io_uring_hi, 1 << 9);

        let offered = init_request(InitFlags::FUSE_OVER_IO_URING | InitFlags::FUSE_ASYNC_READ);
        assert_eq!(flags2_of(&offered, false) & io_uring_hi, 0);
        assert_eq!(flags2_of(&offered, true) & io_uring_hi, io_uring_hi);

        let not_offered = init_request(InitFlags::FUSE_ASYNC_READ);
        assert_eq!(flags2_of(&not_offered, true) & io_uring_hi, 0);
    }
}
