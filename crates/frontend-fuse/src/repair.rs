//! [`RepairLane`]: the write-race repair (plan 42 §5.2.3, W2) — the pages
//! reads may have fetched under an in-flight write-through write are
//! invalidated before that write is answered, from a lane of its own, with
//! a bound on the wait that only runs while the write is alone on its
//! inode.
//!
//! # Why before the reply, and why not from the worker
//!
//! The common way an application sees race 1 is its own `pread` right
//! after `pwrite` returns (`crate::pagecache`'s module doc has the race):
//! a repair written after the reply races that read, which usually wins,
//! and other readers would see new bytes, then old ones again. So the
//! reply waits for the repair. But the repair is a
//! `FUSE_NOTIFY_INVAL_INODE` with a byte range, which locks each folio it
//! drops (`invalidate_inode_pages2_range`) and waits for writeback — and
//! a FUSE worker must never wait for a notification (`kernel_inval`'s
//! module doc in the engine: the answer that unblocks it may be the
//! worker's own). The worker therefore hands the reply here and returns.
//!
//! # Why it cannot deadlock, and what the bound is for
//!
//! The notification takes no inode lock and no `i_rwsem`; it can wait only
//! for a folio lock, for writeback, for `i_mmap` (`unmap_mapping_pages`) or
//! for a `launder_folio` `WRITE` it sends itself — each released by the
//! answer to some request of the inode a worker will give (a `READ`
//! filling the folio, a writeback `WRITE`), or by the answer to the
//! repaired write `W` itself, which this lane holds. The writer of a
//! write-through write holds a lock only on a folio it filled itself, the
//! last one of its request when the copy left it not uptodate; a folio a
//! *read* filled is never one of those — unless it was dropped once more
//! unseen (or a read of it failed and left it not uptodate) and `W`
//! re-created it as a partial tail. Then the repair waits for `W`'s reply:
//! the one cycle, which the bound breaks. With the kernel fix (patch 0001,
//! write-through folios under writeback for the `WRITE`) every recorded
//! page `W` covers waits for `W`'s reply, which is why W2 is off there
//! (`crate::workarounds`).
//!
//! The bound is **in-flight-aware**: a job's clock runs only while it has
//! been entered (a lane thread is writing its notification) *and* no
//! other request of the inode is in progress (`PcEntry::ops_in_flight`,
//! `W` itself excluded once handed here) — then the notification can be
//! waiting only for `W`. While another request is in progress the wait is
//! counted (`repair_wait_ms`) and never ends the job: CPU starvation and
//! daemon pauses delay the reply, they never send it ahead of the repair.
//! Past the bound (`CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS`, default
//! 1000) the watchdog answers `W` — the reply unblocks the notification —
//! and the inode is marked S5 (its next open drops the pages, today's
//! bound). What the count cannot see is a request the kernel has queued
//! that no worker has read yet: the queueing delay, which the bound
//! covers. A queued job is answered early only when every lane thread is
//! inside a job whose clock has run out.
//!
//! The lane writes through the session's [`NotifyGate`]: a detach waits
//! for repairs under way like for any notification, and a job that finds
//! the gate closed answers at once and marks the inode S5. Every job is
//! counted as a bounded deferral until its reply is written, so a detach
//! drains it. A `kill -9` while a lane thread is blocked is the case of
//! `kernel_inval`'s module doc, handled the same way (the zombie reaper).

use crate::adapter::{BoundedHold, Deferred};
use crate::notify::NotifyGate;
use crate::pagecache::{Mark, PageCacheTable};
use constellation_vfs::Ino;
use fuser::INodeNo;
use std::collections::VecDeque;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

/// Lane threads per session.
const LANE_THREADS: usize = 2;

/// The watchdog's tick.
const TICK: Duration = Duration::from_millis(10);

/// The watchdog's tick while a job is on its last page ([`TAIL_GRACE`]
/// is a few ticks of it; the 10 ms tick would add up to 20 ms to it).
const TAIL_TICK: Duration = Duration::from_millis(1);

/// The default bound (`CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS`).
pub const DEFAULT_WAIT: Duration = Duration::from_millis(1000);

/// How long the write's last page may block the repair while nothing
/// else of the inode is in flight before the write is answered (see
/// `Claim::Repair::tail`): blocked there, the repair waits for the lock
/// the writer keeps on its own folio until its reply, a page no read can
/// have made stale. The one other holder of that lock would be a read the
/// daemon has answered and the kernel not yet processed — so the grace is
/// what such a reader may stay unscheduled, and past it the inode is
/// marked S5 all the same.
///
/// Only a synchronous read can be that reader. Readahead (`READ`s sent in
/// the background, `FUSE_ASYNC_READ`) unlocks its folios in the reply's
/// own write, before the reply counts as written and the request leaves
/// `ops_in_flight`; a `read_folio` (a page present but not uptodate, such
/// as a partial write's) unlocks after its task wakes up. 10 ms covers
/// that wake-up on a busy host; it does not cover a reader its cgroup's
/// CPU quota keeps stopped longer (and 100 ms, one default quota period,
/// did not cover every such case either). A write blocked on its own
/// page holds `inode_lock`, so each release costs every writer of the
/// file the grace: 100 ms made `pc_race` with an opener answer a ninth of
/// its writes (plan 42 §8).
const TAIL_GRACE: Duration = Duration::from_millis(10);

/// A late repair is logged at most this often per inode.
const LATE_LOG_EVERY: Duration = Duration::from_secs(60);

/// A reply the lane writes once the repair is done (or the watchdog, at
/// the bound).
pub(crate) type Answer = Box<dyn FnOnce() + Send>;

/// The clock of one job (module doc).
#[derive(Default)]
struct Clock {
    /// A lane thread is writing the job's notification.
    entered: bool,
    /// Since when the write has been alone on its inode, if it is now.
    alone_since: Option<Instant>,
    /// How long it was alone before that.
    alone: Duration,
    /// When the job was handed to the lane.
    queued: Option<Instant>,
    /// The lane is writing the job's last, possibly self-locked page
    /// (`Job::tail`), alone since then for this long.
    in_tail: bool,
    tail_since: Option<Instant>,
    /// The write was answered early on its last page.
    released: bool,
}

impl Clock {
    fn alone_for(&self, now: Instant) -> Duration {
        self.alone + self.alone_since.map_or(Duration::ZERO, |s| now - s)
    }
}

struct Job {
    ino: Ino,
    /// Page ranges `[start, end)`, merged.
    ranges: Vec<(u64, u64)>,
    /// The page invalidated last, on its own (`Claim::Repair::tail`).
    tail: Option<u64>,
    answer: Mutex<Option<Answer>>,
    clock: Mutex<Clock>,
    /// Counted as a bounded deferral until answered.
    _hold: BoundedHold,
}

impl Job {
    /// Take the reply to write it; `None` if it was written already.
    fn take(&self) -> Option<Answer> {
        self.answer.lock().unwrap().take()
    }
}

#[derive(Default)]
struct State {
    queue: VecDeque<Arc<Job>>,
    /// The jobs lane threads are inside.
    active: Vec<Arc<Job>>,
    threads: usize,
    /// The watchdog thread is running (it ends with the lane).
    watching: bool,
    closed: bool,
    /// When a late repair of an inode was last logged.
    late_logged: std::collections::HashMap<Ino, Instant>,
}

struct Inner {
    state: Mutex<State>,
    work: Condvar,
    /// Wakes the watchdog (a job reached its last page).
    watch: Condvar,
    pc: Arc<PageCacheTable>,
    gate: Arc<OnceLock<Arc<NotifyGate>>>,
    deferred: Arc<Deferred>,
    bound: Duration,
}

/// One session's repair lane (see the module doc). Its threads start with
/// its first job and end when the lane is dropped.
pub(crate) struct RepairLane {
    inner: Arc<Inner>,
}

impl Drop for RepairLane {
    fn drop(&mut self) {
        self.inner.state.lock().unwrap().closed = true;
        self.inner.work.notify_all();
    }
}

impl RepairLane {
    pub(crate) fn new(
        pc: Arc<PageCacheTable>,
        gate: Arc<OnceLock<Arc<NotifyGate>>>,
        deferred: Arc<Deferred>,
        bound: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(Inner {
                state: Mutex::default(),
                work: Condvar::new(),
                watch: Condvar::new(),
                pc,
                gate,
                deferred,
                bound,
            }),
        })
    }

    /// Repair `ranges` (pages) of `ino`, then run `answer` (the write's
    /// reply). Returns at once.
    pub(crate) fn submit(
        &self,
        ino: Ino,
        ranges: Vec<(u64, u64)>,
        tail: Option<u64>,
        answer: Answer,
    ) {
        let job = Arc::new(Job {
            ino,
            ranges,
            tail,
            answer: Mutex::new(Some(answer)),
            clock: Mutex::new(Clock {
                queued: Some(Instant::now()),
                ..Default::default()
            }),
            _hold: self.inner.deferred.hold_bounded(),
        });
        let mut st = self.inner.state.lock().unwrap();
        if st.closed {
            drop(st);
            self.answer_unrepaired(&job);
            return;
        }
        st.queue.push_back(job);
        if st.threads == 0 {
            st.threads = LANE_THREADS;
            drop(st);
            self.start();
        } else {
            drop(st);
        }
        self.inner.work.notify_one();
    }

    fn start(&self) {
        // The watchdog first: without it a repair blocked on the writer's
        // own page would never be answered, so no lane runs without it.
        let mut watching = self.inner.state.lock().unwrap().watching;
        if !watching {
            let weak = Arc::downgrade(&self.inner);
            match std::thread::Builder::new()
                .name("fuse-repair-watch".into())
                .spawn(move || watchdog(weak))
            {
                Ok(_) => {
                    watching = true;
                    self.inner.state.lock().unwrap().watching = true;
                }
                Err(error) => {
                    tracing::warn!(%error, "could not start the FUSE write-repair watchdog")
                }
            }
        }
        let mut lanes = 0;
        for i in 0..LANE_THREADS {
            if !watching {
                break;
            }
            let inner = self.inner.clone();
            match std::thread::Builder::new()
                .name(format!("fuse-repair-{i}"))
                .spawn(move || lane(inner))
            {
                Ok(_) => lanes += 1,
                Err(error) => {
                    tracing::warn!(%error, "could not start a FUSE write-repair thread")
                }
            }
        }
        if lanes == 0 {
            // Nothing would ever take a queued job (a thread could not be
            // created: a load condition, so never a hang): answer them
            // unrepaired, S5, and try again with the next job.
            let jobs: Vec<_> = {
                let mut st = self.inner.state.lock().unwrap();
                st.threads = 0;
                st.queue.drain(..).collect()
            };
            for job in jobs {
                self.answer_unrepaired(&job);
            }
        }
    }

    /// Answer `job` without its repair: the inode's pages are not known
    /// current (S5).
    fn answer_unrepaired(&self, job: &Job) {
        answer_unrepaired(&self.inner, job);
    }
}

fn answer_unrepaired(inner: &Inner, job: &Job) {
    inner.pc.mark(job.ino, Mark::S5);
    inner.pc.stats().repairs_late.fetch_add(1, Relaxed);
    if let Some(answer) = job.take() {
        answer();
    }
}

fn lane(inner: Arc<Inner>) {
    loop {
        let job = {
            let mut st = inner.state.lock().unwrap();
            loop {
                if let Some(job) = st.queue.pop_front() {
                    st.active.push(job.clone());
                    break job;
                }
                if st.closed {
                    return;
                }
                st = inner.work.wait(st).unwrap();
            }
        };
        repair(&inner, &job);
        inner
            .state
            .lock()
            .unwrap()
            .active
            .retain(|j| !Arc::ptr_eq(j, &job));
    }
}

fn repair(inner: &Inner, job: &Job) {
    let page = inner.pc.page_size();
    let entered = inner.gate.get().map(|g| (g.clone(), g.enter()));
    let (gate, notifier) = match entered {
        Some((gate, Ok((notifier, _, _)))) => (gate, notifier),
        // Closed for a handover, retired, or a session without a
        // notifier: nothing can be repaired.
        Some((_, Err(_))) | None => {
            answer_unrepaired(inner, job);
            return;
        }
    };
    job.clock.lock().unwrap().entered = true;
    let seq = inner.pc.arm_drop(job.ino, true, false);
    let mut pages = 0;
    for &(start, end) in &job.ranges {
        // Errors only mean there was nothing to drop (`ENOENT`: the kernel
        // holds no such inode any more).
        let _ = notifier.inval_inode(
            INodeNo(job.ino),
            (start * page) as i64,
            ((end - start) * page) as i64,
        );
        pages += end - start;
    }
    if let Some(t) = job.tail {
        job.clock.lock().unwrap().in_tail = true;
        inner.watch.notify_one();
        let _ = notifier.inval_inode(INodeNo(job.ino), (t * page) as i64, page as i64);
        pages += 1;
    }
    gate.exit();
    let answer = job.take();
    if answer.is_some() {
        inner.pc.repair_written_in_time(job.ino, seq);
    } else {
        inner.pc.drop_written(job.ino, seq);
    }
    let waited = job
        .clock
        .lock()
        .unwrap()
        .queued
        .map_or(Duration::ZERO, |q| q.elapsed());
    let stats = inner.pc.stats();
    match answer {
        Some(answer) => {
            answer();
            stats.repairs.fetch_add(1, Relaxed);
            stats.repair_pages.fetch_add(pages, Relaxed);
            let ms = waited.as_millis() as u64;
            stats.repair_wait_ms.fetch_add(ms, Relaxed);
            stats.repair_wait_max_ms.fetch_max(ms, Relaxed);
        }
        None if job.clock.lock().unwrap().released => {
            // Answered early on its last page (`TAIL_GRACE`), the rest
            // repaired in time.
            stats.repairs.fetch_add(1, Relaxed);
            stats.repair_pages.fetch_add(pages, Relaxed);
        }
        None => {
            // The watchdog answered the write at the bound: a reader may
            // have seen the stale page meanwhile.
            inner.pc.mark(job.ino, Mark::S5);
            stats.repairs_late.fetch_add(1, Relaxed);
            let now = Instant::now();
            let mut st = inner.state.lock().unwrap();
            let due = st
                .late_logged
                .get(&job.ino)
                .is_none_or(|at| now.duration_since(*at) >= LATE_LOG_EVERY);
            if due {
                st.late_logged.insert(job.ino, now);
                drop(st);
                tracing::warn!(
                    ino = job.ino,
                    waited = ?waited,
                    "a write-race repair finished after its write was answered at the bound \
                     (CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS): a notification blocked on the \
                     writer's own folio, or a kernel that keeps write-through folios under \
                     writeback with CONSTELLATION_FUSE_WRITE_REPAIR forced on"
                );
            }
        }
    }
}

fn watchdog(inner: Weak<Inner>) {
    let mut sleep = TICK;
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        // Woken early by a lane thread reaching a job's last page.
        drop(
            inner
                .watch
                .wait_timeout(inner.state.lock().unwrap(), sleep)
                .unwrap(),
        );
        if !tick(&inner, Instant::now()) {
            return;
        }
        sleep = next_tick(&inner);
    }
}

/// [`TAIL_TICK`] while a job's write may wait for [`TAIL_GRACE`] (its
/// lane thread is on its last page and the write is not answered yet),
/// else [`TICK`].
fn next_tick(inner: &Inner) -> Duration {
    let active = inner.state.lock().unwrap().active.clone();
    let on_tail = active.iter().any(|job| {
        let c = job.clock.lock().unwrap();
        c.in_tail && !c.released && job.answer.lock().unwrap().is_some()
    });
    if on_tail {
        TAIL_TICK
    } else {
        TICK
    }
}

/// One pass of the watchdog at `now`; `false` once the lane is closed and
/// idle.
fn tick(inner: &Inner, now: Instant) -> bool {
    {
        let (active, queued, closed) = {
            let st = inner.state.lock().unwrap();
            (
                st.active.clone(),
                st.queue.iter().cloned().collect::<Vec<_>>(),
                st.closed,
            )
        };
        if closed && active.is_empty() && queued.is_empty() {
            return false;
        }
        let mut stuck = 0;
        for job in &active {
            let ops = inner.pc.ops_in_flight(job.ino);
            let (expired, release) = {
                let mut c = job.clock.lock().unwrap();
                if !c.entered {
                    (false, false)
                } else {
                    match (ops, c.alone_since) {
                        (0, None) => c.alone_since = Some(now),
                        (0, Some(_)) => {}
                        (_, Some(since)) => {
                            c.alone += now - since;
                            c.alone_since = None;
                        }
                        (_, None) => {}
                    }
                    let release = c.in_tail
                        && match (ops, c.tail_since) {
                            (0, None) => {
                                c.tail_since = Some(now);
                                false
                            }
                            (0, Some(since)) => now - since >= TAIL_GRACE,
                            (_, _) => {
                                c.tail_since = None;
                                false
                            }
                        };
                    (c.alone_for(now) >= inner.bound, release && !c.released)
                }
            };
            if release {
                // Blocked on its last page with nothing else in flight:
                // on the writer's own folio (`TAIL_GRACE`'s doc). Taken
                // under the clock's lock, so the lane, finding the reply
                // gone, sees why.
                let answer = {
                    let mut c = job.clock.lock().unwrap();
                    let answer = job.take();
                    c.released = answer.is_some();
                    answer
                };
                if let Some(answer) = answer {
                    inner.pc.mark(job.ino, Mark::S5);
                    inner.pc.stats().repairs_released.fetch_add(1, Relaxed);
                    answer();
                }
            } else if expired {
                stuck += 1;
                if let Some(answer) = job.take() {
                    // Before the reply: an open answered once the write is
                    // answered must not keep the pages the repair has not
                    // dropped yet (its notification may still be waiting on
                    // a stale folio's lock).
                    inner.pc.mark(job.ino, Mark::S5);
                    answer();
                }
            }
        }
        // Every lane thread inside a job past its bound: the queued jobs'
        // writes are answered now (their repairs follow when a thread
        // frees up, late).
        if stuck >= LANE_THREADS && !queued.is_empty() {
            for job in &queued {
                if let Some(answer) = job.take() {
                    inner.pc.mark(job.ino, Mark::S5);
                    answer();
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pagecache::PageCacheConfig;
    use std::sync::atomic::AtomicU64;

    fn lane_without_gate(bound: Duration) -> (Arc<RepairLane>, Arc<PageCacheTable>) {
        let pc = PageCacheTable::with_page_size(PageCacheConfig::default(), 4096);
        let lane = RepairLane::new(pc.clone(), Arc::default(), Arc::default(), bound);
        (lane, pc)
    }

    /// A closed (here: absent) gate answers at once, marks S5, and counts
    /// the repair as skipped.
    #[test]
    fn a_closed_gate_answers_at_once_with_s5() {
        let (lane, pc) = lane_without_gate(Duration::from_secs(60));
        pc.write_begin(7, 1, false);
        let answered = Arc::new(AtomicU64::new(0));
        let a = answered.clone();
        lane.submit(
            7,
            vec![(0, 1)],
            None,
            Box::new(move || {
                a.fetch_add(1, Relaxed);
            }),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while answered.load(Relaxed) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(answered.load(Relaxed), 1);
        assert_eq!(pc.counts().repairs_late, 1);
        pc.with_entry(7, |e| assert_ne!(e.unwrap().marks, 0, "S5"));
    }

    /// A job past its bound is answered by the watchdog with the inode
    /// marked S5 *before* the reply: an open answered after the write must
    /// not keep pages the blocked repair has not dropped yet. (Checked to
    /// fail with the mark moved after `answer()`, where the lane's late
    /// branch used to set it.)
    #[test]
    fn a_job_past_its_bound_is_answered_with_s5_already_set() {
        let (lane, pc) = lane_without_gate(Duration::from_millis(100));
        // An entry, and the write no longer in flight here (handed to the
        // lane, as `WriteReply` does).
        assert!(pc.write_begin(7, 1, false));
        pc.op_done(7);
        let marked_at_reply = Arc::new(AtomicU64::new(u64::MAX));
        let job = {
            let pc = pc.clone();
            let m = marked_at_reply.clone();
            Arc::new(Job {
                ino: 7,
                ranges: vec![(0, 1)],
                tail: None,
                answer: Mutex::new(Some(Box::new(move || {
                    pc.with_entry(7, |e| m.store(u64::from(e.unwrap().marks), Relaxed));
                }))),
                clock: Mutex::new(Clock::default()),
                _hold: lane.inner.deferred.hold_bounded(),
            })
        };
        let now = Instant::now();
        {
            let mut c = job.clock.lock().unwrap();
            c.entered = true;
            c.alone_since = Some(now);
        }
        lane.inner.state.lock().unwrap().active.push(job.clone());
        assert!(tick(&lane.inner, now + Duration::from_millis(50)));
        assert_eq!(marked_at_reply.load(Relaxed), u64::MAX, "within the bound");
        assert!(tick(&lane.inner, now + Duration::from_millis(150)));
        let marks = marked_at_reply.load(Relaxed);
        assert_ne!(marks, u64::MAX, "answered at the bound");
        assert_ne!(marks, 0, "S5 set before the reply");
    }

    /// A write whose repair is blocked on its last page while nothing else
    /// of the inode is in flight is answered once [`TAIL_GRACE`] has
    /// passed, with S5 set, and the watchdog ticks at [`TAIL_TICK`] only
    /// meanwhile. The writer holds `inode_lock` all that time, so every
    /// writer of the file waits: the answer must come within 15 ms, not at
    /// the 100 ms that cut `pc_race` with an opener to a ninth of its writes
    /// (plan 42 §8; checked to fail with the grace at 100 ms).
    #[test]
    fn a_write_alone_on_its_last_page_is_answered_after_the_grace() {
        let (lane, pc) = lane_without_gate(Duration::from_secs(60));
        assert!(pc.write_begin(7, 1, false));
        pc.op_done(7);
        let answered = Arc::new(AtomicU64::new(0));
        let job = {
            let a = answered.clone();
            Arc::new(Job {
                ino: 7,
                ranges: vec![],
                tail: Some(0),
                answer: Mutex::new(Some(Box::new(move || {
                    a.fetch_add(1, Relaxed);
                }))),
                clock: Mutex::new(Clock {
                    entered: true,
                    in_tail: true,
                    ..Default::default()
                }),
                _hold: lane.inner.deferred.hold_bounded(),
            })
        };
        lane.inner.state.lock().unwrap().active.push(job.clone());
        assert_eq!(next_tick(&lane.inner), TAIL_TICK);
        let now = Instant::now();
        assert!(tick(&lane.inner, now));
        assert!(tick(&lane.inner, now + TAIL_GRACE / 2));
        assert_eq!(answered.load(Relaxed), 0, "within the grace");
        assert!(tick(&lane.inner, now + Duration::from_millis(15)));
        assert_eq!(answered.load(Relaxed), 1, "answered after the grace");
        assert_eq!(pc.counts().repairs_released, 1);
        pc.with_entry(7, |e| assert_ne!(e.unwrap().marks, 0, "S5"));
        assert_eq!(next_tick(&lane.inner), TICK, "nothing left to release");
    }

    /// The clock (module doc): it runs only while the job is entered and
    /// nothing else is in flight on the inode; a job whose thread has not
    /// entered it never expires. (Checked to fail with the `entered` and
    /// the `ops` conditions each removed.)
    #[test]
    fn the_clock_runs_only_while_entered_and_alone() {
        let now = Instant::now();
        let mut c = Clock::default();
        assert_eq!(c.alone_for(now + Duration::from_secs(10)), Duration::ZERO);
        c.entered = true;
        c.alone_since = Some(now);
        assert_eq!(
            c.alone_for(now + Duration::from_millis(30)),
            Duration::from_millis(30)
        );
        c.alone += Duration::from_millis(30);
        c.alone_since = None;
        assert_eq!(
            c.alone_for(now + Duration::from_secs(10)),
            Duration::from_millis(30),
            "stopped while another request is in flight"
        );
    }
}
