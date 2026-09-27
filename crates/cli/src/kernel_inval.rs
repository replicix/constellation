//! Kernel cache invalidation for another node's writes (plan 30 §M7).
//!
//! The FUSE mounts answer lookups and attributes with a 1 s TTL
//! (`fusefs::TTL`), so the kernel keeps serving what it cached — a
//! negative dentry, a file's old size, its old pages — for up to a second
//! after the replica underneath has applied another node's change. The
//! `visibility-after-burst` scenario measured exactly that: a marker file
//! whose create and data happened to ship in different segments was
//! listed, read back as empty, and then stayed empty for a whole TTL even
//! though its data arrived ~100 ms later — so cross-node visibility had a
//! ~1.1 s tail that no amount of faster log delivery could remove.
//!
//! Every foreign segment the replica applies is therefore turned into
//! `FUSE_NOTIFY_INVAL_*` notifications for what its records touched: the
//! dentries it created or removed, the parents whose listing and times
//! changed, and the inodes whose attributes or content changed.
//!
//! Only a replica's *foreign* applies are reported (a local write went
//! through this kernel already) — and, just as foreign to this kernel,
//! another node's op executed here as the holder, a delegate or from the
//! inbox, a delegate's transaction appended here as the root, and a
//! transaction streamed here ahead of the log (`Replica::
//! note_foreign_executed`; EC2 campaign 4 B-1: a holder served another
//! node's `rename` over git's `refs/heads/master` from its caches for up
//! to the TTL). Views mounted on a subtree translate their root; snapshot
//! views are frozen and never registered.
//! `CONSTELLATION_KERNEL_INVALIDATE=0` turns it off (the TTL bound then
//! applies as before).
//!
//! # A notification can wedge in the kernel
//!
//! A notification is a `write(2)` on `/dev/fuse` that the kernel serves
//! synchronously: `FUSE_NOTIFY_INVAL_ENTRY` takes the parent directory's
//! `i_rwsem` (`fuse_reverse_inval_entry`: `inode_lock_nested`, a plain
//! `down_write` — not interruptible, not killable) under `fc->killsb`;
//! `FUSE_NOTIFY_INVAL_INODE` with a byte range locks the file's pages
//! (`invalidate_inode_pages2_range`). The VFS holds that very `i_rwsem`
//! while a `lookup`, `create`, `mkdir`, `unlink`, `rename`, `readdir` or a
//! directory `setattr` is *in flight* — from before the request is queued
//! to this daemon until this daemon answers it — and a `read`/`write`
//! keeps its pages locked the same way. So a notification for a directory
//! with a request in flight blocks until that request is answered:
//!
//! - **Never answered from a thread that waits on the notification.** The
//!   answer comes from a FUSE worker, possibly by way of the sync task, so
//!   neither may ever wait for this thread. Notifications are sent from
//!   one dedicated thread, fed through a queue nothing blocks on; the one
//!   waiter (`InodeInvalidator::invalidate_and_wait`, the lock-grant path)
//!   is bounded and told when its notification was dropped instead. The
//!   send happens with no lock of this module held, so a FUSE worker's
//!   `InFlightGuard` never queues behind a notification in the kernel.
//! - **Not issued while a request on the same inode is known to be in
//!   flight** (`InFlight`, maintained by the FUSE handlers): the
//!   notification is held back and sent as soon as the last such request
//!   is answered, so the thread spends almost no time blocked in the
//!   kernel. What it cannot see is a request the kernel has queued that no
//!   FUSE worker has read yet (all workers busy) — that residual window is
//!   the request's queueing delay, not its service time.
//! - **Dropped once older than the TTL.** A held-back notification only
//!   matters for what the kernel cached *before* the apply that queued it,
//!   and everything cached before then has expired one TTL later. So a
//!   notification held back past `DEFER_MAX` is dropped, not sent — the
//!   TTL bound has done its job, and sending it would only park this
//!   thread behind a request for that request's whole service time.
//!
//! Why this matters beyond latency (EC2 campaign 6, finding B-1): a
//! daemon `kill -9`ed while this thread was blocked inside such a write
//! became a zombie forever. The kernel ends a dead daemon's requests only
//! when its last `/dev/fuse` descriptor closes, the descriptors close only
//! when the last thread exits, and the blocked thread cannot exit until
//! the in-flight request it waits on ends — a cycle only an explicit
//! `/sys/fs/fuse/connections/<n>/abort` breaks (`daemon_lock::
//! abort_stale_mounts` does that at takeover). Keeping this thread out of
//! the kernel keeps the window for that cycle to the queueing delay.
//!
//! A watchdog logs a notification blocked in the kernel longer than
//! `CONSTELLATION_KERNEL_INVAL_STALL_S` (default 5 s).

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::LogRecord;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// How long a notification is held back for a request in flight on its
/// inode before it is dropped as redundant: one lookup/attribute TTL
/// (everything the kernel cached before the apply has expired by then)
/// plus a margin for the apply-to-queue delay.
pub const DEFER_MAX: Duration = Duration::from_millis(crate::fusefs::TTL.as_millis() as u64 + 500);

/// The watchdog's period.
const WATCHDOG_TICK: Duration = Duration::from_secs(1);

/// The watchdog repeats its warning about one stalled notification this
/// often.
const WATCHDOG_REPEAT: Duration = Duration::from_secs(30);

/// One thing to invalidate, in replica inode numbers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Inval {
    /// A name in a directory (created, removed, renamed).
    Entry { parent: Ino, name: String },
    /// An inode's attributes, and with `data` its cached pages too.
    Inode { ino: Ino, data: bool },
}

impl Inval {
    /// The inode whose in-flight requests can block this notification in
    /// the kernel — `None` when nothing can (an attribute-only
    /// invalidation takes no sleeping lock).
    fn gate(&self) -> Option<Ino> {
        match self {
            Inval::Entry { parent, .. } => Some(*parent),
            Inval::Inode { ino, data: true } => Some(*ino),
            Inval::Inode { data: false, .. } => None,
        }
    }
}

impl std::fmt::Display for Inval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Inval::Entry { parent, name } => write!(f, "entry {name:?} in directory {parent}"),
            Inval::Inode { ino, data: true } => write!(f, "attributes and pages of inode {ino}"),
            Inval::Inode { ino, data: false } => write!(f, "attributes of inode {ino}"),
        }
    }
}

/// Where a view's notifications go: `fuser::Notifier` in the daemon, a
/// recorder in tests.
pub trait NotifySink: Send + 'static {
    fn inval_entry(&self, parent: Ino, name: &str) -> std::io::Result<()>;
    /// `data`: drop the cached pages too, not only the attributes.
    fn inval_inode(&self, ino: Ino, data: bool) -> std::io::Result<()>;
}

impl NotifySink for fuser::Notifier {
    fn inval_entry(&self, parent: Ino, name: &str) -> std::io::Result<()> {
        fuser::Notifier::inval_entry(self, fuser::INodeNo(parent), OsStr::new(name))
    }

    fn inval_inode(&self, ino: Ino, data: bool) -> std::io::Result<()> {
        fuser::Notifier::inval_inode(self, fuser::INodeNo(ino), if data { 0 } else { -1 }, 0)
    }
}

enum Msg {
    Register {
        id: u64,
        sink: Box<dyn NotifySink>,
        view_root: Ino,
    },
    Unregister(u64),
    Batch(Vec<Inval>),
    /// Plan 30 §M14: a batch someone waits for (`true` once sent, `false`
    /// when dropped).
    Waited(Vec<Inval>, mpsc::Sender<bool>),
}

struct State {
    queue: Vec<Msg>,
    /// Requests in flight per replica inode (`InFlight`).
    inflight: HashMap<Ino, u32>,
    /// The invalidation thread holds something back for an inode with
    /// requests in flight: an `InFlightGuard` that brings a count to zero
    /// wakes it.
    gated: bool,
}

/// Counters, for the log and the tests.
#[derive(Default)]
pub struct Stats {
    pub sent: AtomicU64,
    pub deferred: AtomicU64,
    pub dropped: AtomicU64,
}

struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    /// The notification being written to the kernel right now, and since
    /// when (the watchdog's view).
    busy: Mutex<Option<(Instant, Inval)>>,
    closed: AtomicBool,
    stats: Stats,
    defer_max: Duration,
}

impl Shared {
    fn push(&self, msg: Msg) {
        self.state.lock().unwrap().queue.push(msg);
        self.wake.notify_one();
    }

    /// How long the current notification has been blocked in the kernel,
    /// and what it is.
    fn stalled_for(&self) -> Option<(Duration, Inval)> {
        self.busy
            .lock()
            .unwrap()
            .as_ref()
            .map(|(since, what)| (since.elapsed(), what.clone()))
    }

    fn inflight_of(&self, ino: Ino) -> u32 {
        self.state
            .lock()
            .unwrap()
            .inflight
            .get(&ino)
            .copied()
            .unwrap_or(0)
    }
}

/// The node's invalidation thread (one for every mounted view).
pub struct KernelInvalidator {
    shared: Arc<Shared>,
}

/// `CONSTELLATION_KERNEL_INVALIDATE` (default on).
pub fn enabled() -> bool {
    !matches!(
        std::env::var("CONSTELLATION_KERNEL_INVALIDATE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false"
    )
}

/// `CONSTELLATION_KERNEL_INVAL_STALL_S`: a notification blocked in the
/// kernel longer than this is logged (default 5 s).
fn stall_warn_after() -> Duration {
    Duration::from_secs(
        std::env::var("CONSTELLATION_KERNEL_INVAL_STALL_S")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(5),
    )
}

impl KernelInvalidator {
    pub fn start() -> Self {
        Self::start_with(DEFER_MAX)
    }

    /// [`Self::start`] with the hold-back bound of the tests.
    pub fn start_with(defer_max: Duration) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: Vec::new(),
                inflight: HashMap::new(),
                gated: false,
            }),
            wake: Condvar::new(),
            busy: Mutex::new(None),
            closed: AtomicBool::new(false),
            stats: Stats::default(),
            defer_max,
        });
        {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("kernel-inval".into())
                .spawn(move || run(shared))
                .expect("spawning the kernel invalidation thread");
        }
        {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("kernel-inval-watchdog".into())
                .spawn(move || watchdog(shared))
                .expect("spawning the kernel invalidation watchdog");
        }
        Self { shared }
    }

    /// A view mounted with `view_root` as its root directory.
    pub fn register(&self, id: u64, sink: impl NotifySink, view_root: Ino) {
        self.shared.push(Msg::Register {
            id,
            sink: Box::new(sink),
            view_root,
        });
    }

    pub fn unregister(&self, id: u64) {
        self.shared.push(Msg::Unregister(id));
    }

    /// Plan 30 §M14: a handle for the lock path, which drops one file's
    /// pages and attributes after a lock grant.
    pub fn inodes(&self) -> InodeInvalidator {
        InodeInvalidator {
            shared: self.shared.clone(),
        }
    }

    /// The FUSE handlers' registry of requests in flight.
    pub fn inflight(&self) -> InFlight {
        InFlight(Some(self.shared.clone()))
    }

    /// The hook for `Meta::set_foreign_apply_hook`.
    pub fn hook(&self) -> constellation_meta::ForeignApplyHook {
        let shared = self.shared.clone();
        Box::new(move |records: &[LogRecord]| {
            let batch = invalidations(records);
            if !batch.is_empty() {
                shared.push(Msg::Batch(batch));
            }
        })
    }

    #[cfg(test)]
    fn stats(&self) -> &Stats {
        &self.shared.stats
    }

    /// How long the current notification has been blocked in the kernel.
    #[cfg(test)]
    fn stalled_for(&self) -> Option<(Duration, Inval)> {
        self.shared.stalled_for()
    }
}

impl Drop for KernelInvalidator {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.wake.notify_all();
    }
}

/// Plan 30 §M14: invalidates one file (by replica inode) in every view,
/// waiting — bounded — for the notifications to be sent. A lock grant
/// must not be answered while the kernel may still serve pages written
/// before the previous holder's flush; the bound keeps a notification
/// held back for a request in flight on the file from holding the lock
/// up indefinitely (the lock request itself is not counted: it holds no
/// page).
#[derive(Clone)]
pub struct InodeInvalidator {
    shared: Arc<Shared>,
}

impl InodeInvalidator {
    /// Queue the invalidation of one file without waiting for it.
    pub fn invalidate(&self, ino: Ino) {
        self.shared
            .push(Msg::Batch(vec![Inval::Inode { ino, data: true }]));
    }

    /// `true` once sent; `false` on timeout, or when it was dropped for a
    /// request in flight on the file that outlasted the TTL.
    pub fn invalidate_and_wait(&self, ino: Ino, timeout: Duration) -> bool {
        let (done, wait) = mpsc::channel();
        self.shared
            .push(Msg::Waited(vec![Inval::Inode { ino, data: true }], done));
        matches!(wait.recv_timeout(timeout), Ok(true))
    }
}

/// The FUSE handlers' registry of requests in flight, by replica inode:
/// a notification for an inode with a request in flight is held back
/// (module doc). Cheap to clone; `disabled` counts nothing.
#[derive(Clone, Default)]
pub struct InFlight(Option<Arc<Shared>>);

impl InFlight {
    pub fn disabled() -> Self {
        Self(None)
    }

    /// A request on `inos` (one, or the two directories of a `rename`
    /// or `link`) is in flight until the guard drops. Only requests
    /// during which the kernel holds a lock a notification needs are
    /// registered: those holding the directory's `i_rwsem` (`lookup`,
    /// `readdir`, every namespace mutation, `setattr`) and those holding
    /// pages (`read`, `write`, `setattr`, `fallocate`).
    pub fn enter(&self, inos: &[Ino]) -> InFlightGuard {
        let mut held = [0 as Ino; 2];
        let n = inos.len().min(2);
        held[..n].copy_from_slice(&inos[..n]);
        if let Some(shared) = &self.0 {
            let mut st = shared.state.lock().unwrap();
            for ino in &held[..n] {
                *st.inflight.entry(*ino).or_insert(0) += 1;
            }
        }
        InFlightGuard {
            shared: self.0.clone(),
            inos: held,
            n,
        }
    }

    #[cfg(test)]
    fn count(&self, ino: Ino) -> u32 {
        self.0.as_ref().map_or(0, |s| s.inflight_of(ino))
    }
}

/// One request in flight (`InFlight::enter`).
pub struct InFlightGuard {
    shared: Option<Arc<Shared>>,
    inos: [Ino; 2],
    n: usize,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let Some(shared) = &self.shared else {
            return;
        };
        let mut st = shared.state.lock().unwrap();
        let mut idle = false;
        for ino in &self.inos[..self.n] {
            if let Some(c) = st.inflight.get_mut(ino) {
                *c -= 1;
                if *c == 0 {
                    st.inflight.remove(ino);
                    idle = true;
                }
            }
        }
        if idle && st.gated {
            drop(st);
            shared.wake.notify_one();
        }
    }
}

/// What `records` make stale in a kernel's caches.
fn invalidations(records: &[LogRecord]) -> Vec<Inval> {
    let mut out = BTreeSet::new();
    let entry = |out: &mut BTreeSet<Inval>, parent: Ino, name: &str| {
        out.insert(Inval::Entry {
            parent,
            name: name.to_string(),
        });
        out.insert(Inval::Inode {
            ino: parent,
            data: true,
        });
    };
    for rec in records {
        match rec {
            LogRecord::Mkdir { parent, name, .. }
            | LogRecord::Create { parent, name, .. }
            | LogRecord::Symlink { parent, name, .. }
            | LogRecord::Mknod { parent, name, .. }
            | LogRecord::Unlink { parent, name, .. }
            | LogRecord::Rmdir { parent, name, .. } => entry(&mut out, *parent, name),
            LogRecord::Link {
                ino, parent, name, ..
            } => {
                entry(&mut out, *parent, name);
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: false,
                });
            }
            LogRecord::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                entry(&mut out, *parent, name);
                entry(&mut out, *new_parent, new_name);
            }
            LogRecord::Setattr { ino, size, .. } => {
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: size.is_some(),
                });
            }
            LogRecord::WriteManifest { ino, .. } => {
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: true,
                });
            }
            LogRecord::SetXattr { ino, .. } | LogRecord::RemoveXattr { ino, .. } => {
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: false,
                });
            }
            // Atime is best effort and order free; the rest carry no
            // namespace change a kernel could have cached.
            _ => {}
        }
    }
    out.into_iter().collect()
}

/// A replica inode as `view_root`'s view numbers it (`FuseFs::real_ino`'s
/// inverse), or `None` when the view cannot see it.
fn in_view(ino: Ino, view_root: Ino) -> Option<Ino> {
    if ino == view_root {
        Some(ROOT_INO)
    } else if ino == ROOT_INO {
        None
    } else {
        Some(ino)
    }
}

struct View {
    id: u64,
    sink: Box<dyn NotifySink>,
    view_root: Ino,
}

/// A notification not sent yet: since when (the latest apply that asked
/// for it — what the kernel cached before then is what it is for), and
/// who waits for it.
struct Pending {
    since: Instant,
    waiters: Vec<mpsc::Sender<bool>>,
}

fn merge(
    pending: &mut BTreeMap<Inval, Pending>,
    batch: Vec<Inval>,
    now: Instant,
    waiter: Option<mpsc::Sender<bool>>,
) {
    for inval in batch {
        let p = pending.entry(inval).or_insert_with(|| Pending {
            since: now,
            waiters: Vec::new(),
        });
        p.since = now;
        if let Some(w) = &waiter {
            p.waiters.push(w.clone());
        }
    }
}

fn run(shared: Arc<Shared>) {
    let mut views: Vec<View> = Vec::new();
    let mut pending: BTreeMap<Inval, Pending> = BTreeMap::new();
    let mut to_send: Vec<Inval> = Vec::new();
    let mut to_drop: Vec<Inval> = Vec::new();
    loop {
        // Decide under the lock, act outside it: a FUSE worker's
        // `InFlightGuard` must never wait behind a notification that is
        // itself waiting for that worker's answer.
        {
            let mut st = shared.state.lock().unwrap();
            loop {
                if shared.closed.load(Ordering::SeqCst) {
                    return;
                }
                let now = Instant::now();
                for msg in st.queue.drain(..) {
                    match msg {
                        Msg::Register {
                            id,
                            sink,
                            view_root,
                        } => views.push(View {
                            id,
                            sink,
                            view_root,
                        }),
                        Msg::Unregister(id) => views.retain(|v| v.id != id),
                        Msg::Batch(batch) => merge(&mut pending, batch, now, None),
                        Msg::Waited(batch, done) => merge(&mut pending, batch, now, Some(done)),
                    }
                }
                to_send.clear();
                to_drop.clear();
                let mut next_deadline: Option<Instant> = None;
                st.gated = false;
                for (inval, p) in &pending {
                    let busy = inval
                        .gate()
                        .is_some_and(|ino| st.inflight.get(&ino).is_some_and(|c| *c > 0));
                    if !busy {
                        to_send.push(inval.clone());
                    } else if now.duration_since(p.since) >= shared.defer_max {
                        to_drop.push(inval.clone());
                    } else {
                        st.gated = true;
                        let deadline = p.since + shared.defer_max;
                        next_deadline = Some(next_deadline.map_or(deadline, |d| d.min(deadline)));
                    }
                }
                if !to_send.is_empty() || !to_drop.is_empty() {
                    break;
                }
                st = match next_deadline {
                    Some(deadline) => {
                        let wait = deadline.saturating_duration_since(Instant::now());
                        shared.wake.wait_timeout(st, wait).unwrap().0
                    }
                    None => shared.wake.wait(st).unwrap(),
                };
            }
        }
        for inval in to_send.drain(..) {
            let Some(p) = pending.remove(&inval) else {
                continue;
            };
            *shared.busy.lock().unwrap() = Some((Instant::now(), inval.clone()));
            send(&views, &inval);
            *shared.busy.lock().unwrap() = None;
            shared.stats.sent.fetch_add(1, Ordering::Relaxed);
            for w in p.waiters {
                let _ = w.send(true);
            }
        }
        for inval in to_drop.drain(..) {
            let Some(p) = pending.remove(&inval) else {
                continue;
            };
            shared.stats.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                target: "constellation::kernel_inval",
                what = %inval,
                held_back_for = ?shared.defer_max,
                "kernel cache invalidation dropped: a request was in flight on its inode for \
                 the whole TTL, so nothing the kernel cached before it is left to drop"
            );
            for w in p.waiters {
                let _ = w.send(false);
            }
        }
        if !pending.is_empty() {
            shared.stats.deferred.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn send(views: &[View], inval: &Inval) {
    for view in views {
        // ENOENT (nothing cached) is the common answer;
        // every error only means there was nothing to drop.
        let _ = match inval {
            Inval::Entry { parent, name } => match in_view(*parent, view.view_root) {
                Some(p) => view.sink.inval_entry(p, name),
                None => Ok(()),
            },
            Inval::Inode { ino, data } => match in_view(*ino, view.view_root) {
                Some(i) => view.sink.inval_inode(i, *data),
                None => Ok(()),
            },
        };
    }
}

/// The watchdog's verdict on one tick: a warning is due when the
/// notification has been in the kernel longer than `after` and none was
/// logged for it in the last `WATCHDOG_REPEAT`.
fn stall_warning(
    stalled: Option<(Duration, Inval)>,
    after: Duration,
    last_warned: &mut Option<(Inval, Instant)>,
    now: Instant,
) -> Option<(Duration, Inval)> {
    let (for_how_long, what) = stalled?;
    if for_how_long < after {
        *last_warned = None;
        return None;
    }
    if let Some((warned_what, at)) = last_warned {
        if *warned_what == what && now.duration_since(*at) < WATCHDOG_REPEAT {
            return None;
        }
    }
    *last_warned = Some((what.clone(), now));
    Some((for_how_long, what))
}

fn watchdog(shared: Arc<Shared>) {
    let after = stall_warn_after();
    let mut last_warned: Option<(Inval, Instant)> = None;
    loop {
        std::thread::sleep(WATCHDOG_TICK);
        if shared.closed.load(Ordering::SeqCst) {
            return;
        }
        if let Some((for_how_long, what)) = stall_warning(
            shared.stalled_for(),
            after,
            &mut last_warned,
            Instant::now(),
        ) {
            let inflight = what.gate().map(|ino| shared.inflight_of(ino));
            tracing::warn!(
                target: "constellation::kernel_inval",
                what = %what,
                blocked_for = ?for_how_long,
                requests_in_flight_on_it = ?inflight,
                "a kernel cache invalidation has been blocked in the kernel: a FUSE request on \
                 that inode is holding the lock it needs and has not been answered yet"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// A sink that records what it was told, optionally blocking every
    /// call until released.
    #[derive(Clone, Default)]
    struct Recorder {
        got: Arc<StdMutex<Vec<String>>>,
        block: Option<Arc<(StdMutex<bool>, Condvar)>>,
    }

    impl Recorder {
        fn blocking() -> Self {
            Self {
                got: Default::default(),
                block: Some(Arc::new((StdMutex::new(true), Condvar::new()))),
            }
        }

        fn release(&self) {
            if let Some(b) = &self.block {
                *b.0.lock().unwrap() = false;
                b.1.notify_all();
            }
        }

        fn wait_if_blocked(&self) {
            if let Some(b) = &self.block {
                let mut blocked = b.0.lock().unwrap();
                while *blocked {
                    blocked = b.1.wait(blocked).unwrap();
                }
            }
        }

        fn got(&self) -> Vec<String> {
            self.got.lock().unwrap().clone()
        }

        fn wait_for(&self, n: usize, within: Duration) -> Vec<String> {
            let start = Instant::now();
            while self.got.lock().unwrap().len() < n && start.elapsed() < within {
                std::thread::sleep(Duration::from_millis(5));
            }
            self.got()
        }
    }

    impl NotifySink for Recorder {
        fn inval_entry(&self, parent: Ino, name: &str) -> std::io::Result<()> {
            self.wait_if_blocked();
            self.got
                .lock()
                .unwrap()
                .push(format!("entry {parent} {name}"));
            Ok(())
        }

        fn inval_inode(&self, ino: Ino, data: bool) -> std::io::Result<()> {
            self.wait_if_blocked();
            self.got
                .lock()
                .unwrap()
                .push(format!("inode {ino} data={data}"));
            Ok(())
        }
    }

    fn started(defer_max: Duration) -> (KernelInvalidator, Recorder) {
        let k = KernelInvalidator::start_with(defer_max);
        let rec = Recorder::default();
        k.register(1, rec.clone(), ROOT_INO);
        (k, rec)
    }

    fn entry(parent: Ino, name: &str) -> Inval {
        Inval::Entry {
            parent,
            name: name.into(),
        }
    }

    #[test]
    fn a_create_and_its_data_invalidate_the_entry_the_parent_and_the_file() {
        let got = invalidations(&[
            LogRecord::Create {
                parent: 5,
                name: "m1".into(),
                ino: 9,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 1,
            },
            LogRecord::WriteManifest {
                ino: 9,
                base_manifest: None,
                manifest: Vec::new(),
                size: 8,
                time_ns: 2,
            },
        ]);
        assert_eq!(
            got,
            vec![
                Inval::Entry {
                    parent: 5,
                    name: "m1".into()
                },
                Inval::Inode { ino: 5, data: true },
                Inval::Inode { ino: 9, data: true },
            ]
        );
    }

    #[test]
    fn a_subtree_view_renumbers_its_root_and_hides_the_real_root() {
        assert_eq!(in_view(42, 42), Some(ROOT_INO));
        assert_eq!(in_view(ROOT_INO, 42), None);
        assert_eq!(in_view(7, 42), Some(7));
        assert_eq!(in_view(ROOT_INO, ROOT_INO), Some(ROOT_INO));
    }

    #[test]
    fn a_batch_is_sent_to_every_view_in_its_numbering() {
        let (k, rec) = started(DEFER_MAX);
        let sub = Recorder::default();
        k.register(2, sub.clone(), 42);
        k.shared.push(Msg::Batch(vec![
            entry(42, "x"),
            Inval::Inode {
                ino: 42,
                data: true,
            },
        ]));
        assert_eq!(
            rec.wait_for(2, Duration::from_secs(5)),
            vec!["entry 42 x", "inode 42 data=true"]
        );
        assert_eq!(
            sub.wait_for(2, Duration::from_secs(5)),
            vec!["entry 1 x", "inode 1 data=true"]
        );
        assert_eq!(k.stats().sent.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn a_notification_is_held_back_while_a_request_is_in_flight_on_its_inode_and_sent_after() {
        let (k, rec) = started(DEFER_MAX);
        let inflight = k.inflight();
        let guard = inflight.enter(&[7]);
        assert_eq!(inflight.count(7), 1);
        k.shared.push(Msg::Batch(vec![entry(7, "a")]));
        // Something unrelated goes through meanwhile.
        k.shared.push(Msg::Batch(vec![entry(8, "b")]));
        assert_eq!(rec.wait_for(1, Duration::from_secs(5)), vec!["entry 8 b"]);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(rec.got(), vec!["entry 8 b"]);
        drop(guard);
        assert_eq!(inflight.count(7), 0);
        assert_eq!(
            rec.wait_for(2, Duration::from_secs(5)),
            vec!["entry 8 b", "entry 7 a"]
        );
    }

    #[test]
    fn two_requests_on_one_inode_hold_it_back_until_the_last_one_leaves() {
        let (k, rec) = started(DEFER_MAX);
        let inflight = k.inflight();
        let g1 = inflight.enter(&[7]);
        let g2 = inflight.enter(&[7, 7]);
        assert_eq!(inflight.count(7), 3);
        k.shared.push(Msg::Batch(vec![entry(7, "a")]));
        drop(g1);
        std::thread::sleep(Duration::from_millis(100));
        assert!(rec.got().is_empty());
        drop(g2);
        assert_eq!(rec.wait_for(1, Duration::from_secs(5)), vec!["entry 7 a"]);
    }

    #[test]
    fn an_attribute_only_invalidation_is_never_held_back() {
        let (k, rec) = started(DEFER_MAX);
        let _guard = k.inflight().enter(&[7]);
        k.shared.push(Msg::Batch(vec![Inval::Inode {
            ino: 7,
            data: false,
        }]));
        assert_eq!(
            rec.wait_for(1, Duration::from_secs(5)),
            vec!["inode 7 data=false"]
        );
    }

    #[test]
    fn a_notification_held_back_past_the_bound_is_dropped_and_its_waiter_told() {
        let (k, rec) = started(Duration::from_millis(200));
        let _guard = k.inflight().enter(&[7]);
        let t = Instant::now();
        assert!(!k.inodes().invalidate_and_wait(7, Duration::from_secs(5)));
        assert!(t.elapsed() >= Duration::from_millis(200));
        assert!(t.elapsed() < Duration::from_secs(4));
        assert!(rec.got().is_empty());
        assert_eq!(k.stats().dropped.load(Ordering::Relaxed), 1);
        assert_eq!(k.stats().sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_waited_notification_answers_true_once_sent() {
        let (k, rec) = started(DEFER_MAX);
        assert!(k.inodes().invalidate_and_wait(9, Duration::from_secs(5)));
        assert_eq!(rec.got(), vec!["inode 9 data=true"]);
    }

    #[test]
    fn a_re_queued_notification_keeps_one_pending_entry_and_the_latest_deadline() {
        let (k, rec) = started(Duration::from_millis(300));
        let guard = k.inflight().enter(&[7]);
        k.shared.push(Msg::Batch(vec![entry(7, "a")]));
        std::thread::sleep(Duration::from_millis(200));
        k.shared.push(Msg::Batch(vec![entry(7, "a")]));
        std::thread::sleep(Duration::from_millis(200));
        // 400 ms after the first, 200 ms after the second: not dropped.
        assert_eq!(k.stats().dropped.load(Ordering::Relaxed), 0);
        drop(guard);
        assert_eq!(rec.wait_for(1, Duration::from_secs(5)), vec!["entry 7 a"]);
        assert_eq!(k.stats().sent.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn duplicates_across_batches_coalesce_while_held_back() {
        let (k, rec) = started(DEFER_MAX);
        let guard = k.inflight().enter(&[7]);
        for _ in 0..50 {
            k.shared.push(Msg::Batch(vec![
                entry(7, "a"),
                Inval::Inode { ino: 7, data: true },
            ]));
        }
        std::thread::sleep(Duration::from_millis(100));
        drop(guard);
        let got = rec.wait_for(2, Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(got, vec!["entry 7 a", "inode 7 data=true"]);
        assert_eq!(rec.got().len(), 2);
    }

    #[test]
    fn a_notification_blocked_in_the_kernel_blocks_no_request_and_is_visible_to_the_watchdog() {
        let k = KernelInvalidator::start_with(DEFER_MAX);
        let rec = Recorder::blocking();
        k.register(1, rec.clone(), ROOT_INO);
        k.shared.push(Msg::Batch(vec![entry(7, "a")]));
        let start = Instant::now();
        while k.stalled_for().is_none() && start.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(50));
        let (for_how_long, what) = k.stalled_for().expect("a notification in the kernel");
        assert!(for_how_long >= Duration::from_millis(50));
        assert_eq!(what, entry(7, "a"));
        // FUSE workers come and go freely meanwhile.
        let inflight = k.inflight();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let g = inflight.enter(&[7]);
            drop(g);
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(1))
            .expect("a request on the inode in flight neither queues behind the notification");
        // And so do producers.
        k.inodes().invalidate(7);
        rec.release();
        assert_eq!(
            rec.wait_for(2, Duration::from_secs(5)),
            vec!["entry 7 a", "inode 7 data=true"]
        );
        assert!(k.stalled_for().is_none());
    }

    #[test]
    fn the_watchdog_warns_once_past_the_bound_and_again_after_the_repeat_period() {
        let now = Instant::now();
        let mut last = None;
        let a = entry(7, "a");
        assert!(stall_warning(None, Duration::from_secs(5), &mut last, now).is_none());
        assert!(stall_warning(
            Some((Duration::from_secs(4), a.clone())),
            Duration::from_secs(5),
            &mut last,
            now
        )
        .is_none());
        assert!(stall_warning(
            Some((Duration::from_secs(6), a.clone())),
            Duration::from_secs(5),
            &mut last,
            now
        )
        .is_some());
        assert!(stall_warning(
            Some((Duration::from_secs(7), a.clone())),
            Duration::from_secs(5),
            &mut last,
            now + Duration::from_secs(1)
        )
        .is_none());
        assert!(stall_warning(
            Some((Duration::from_secs(40), a.clone())),
            Duration::from_secs(5),
            &mut last,
            now + WATCHDOG_REPEAT + Duration::from_secs(1)
        )
        .is_some());
        // A different notification stalled: its own warning.
        assert!(stall_warning(
            Some((Duration::from_secs(6), entry(8, "b"))),
            Duration::from_secs(5),
            &mut last,
            now + WATCHDOG_REPEAT + Duration::from_secs(2)
        )
        .is_some());
    }

    #[test]
    fn unregistering_a_view_stops_its_notifications() {
        let (k, rec) = started(DEFER_MAX);
        k.unregister(1);
        k.shared.push(Msg::Batch(vec![entry(7, "a")]));
        std::thread::sleep(Duration::from_millis(100));
        assert!(rec.got().is_empty());
        assert_eq!(k.stats().sent.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_disabled_registry_counts_nothing() {
        let inflight = InFlight::disabled();
        let g = inflight.enter(&[7, 8]);
        assert_eq!(inflight.count(7), 0);
        drop(g);
    }
}
