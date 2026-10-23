//! Plan 42 F21: which inodes an attribute reply may still be on its way
//! to the kernel for, so that W3b writes its notice only where it closes
//! race 2b.
//!
//! The notice (`FUSE_NOTIFY_INVAL_INODE`, attributes only) bumps the
//! inode's `attr_version`, which makes the kernel reject any `GETATTR`,
//! `LOOKUP` or `SETATTR` reply whose request was sent before it. It
//! invalidates the inode's whole attribute mask, so the next path walk
//! through a directory refetches its mode (`default_permissions`): one
//! `GETATTR` per notice. A notice does something only for a reply the
//! kernel has not installed yet:
//!
//! - one **being generated**: the request arrived before the change, the
//!   reply is written after it;
//! - one **written but not installed**: a task installs a reply in the
//!   syscall that sent the request, so until the same task sends its next
//!   request the reply may still be on its way.
//!
//! Both are tracked without a lock on the common path:
//!
//! - A namespace operation that changed inodes records them in a ring
//!   ([`Changes`]) and counts them. An attribute request takes the count
//!   when it arrives ([`Arrival`]); when it replies for inode `i` and the
//!   count has moved, it looks `i` up in the ring and, if a change of `i`
//!   was recorded since, writes the notice itself, before its reply.
//! - A reply for inode `i` to task `t` is a mark `t -> i`. A namespace
//!   operation finds the marks of the inodes it changed (an index by
//!   inode), writes a notice for each, and then discharges them
//!   ([`AttrWatch::release`]): the bump rejects every reply sent before
//!   it. Not before the notice is written: a second change of the inode in
//!   between must still find the mark and write its own notice before its
//!   reply.
//!
//! Each side stores its record, issues a full fence and then reads the
//! other's (store buffering), so at least one of them sees the other. A
//! reply that did not read the old state gets a needless notice, never a
//! missing one.
//!
//! **A mark is never dropped by time.** How late the kernel installs a
//! reply is not the daemon's to know: over io_uring the ring thread commits
//! it on its next pass, after every other request it reaped with it was
//! handled (handlers block), and a task can wait for a CPU longer than any
//! bound one picks (a throttled or starved cgroup, a paused VM). Under
//! load only latency may degrade, so a mark goes only on evidence that the
//! reply was consumed:
//!
//! - the issuing task's next attribute or namespace request
//!   ([`AttrWatch::arrive`]): a task has one request outstanding, so it
//!   sends the next after the syscall that installed the reply returned;
//! - a notice written for the inode ([`AttrWatch::release`]);
//! - the task's exit: a task that is gone installs nothing. Not tracked
//!   by an event but looked up when it matters: a change that finds a mark
//!   asks the system whether its task still exists (`kill(tid, 0)` in the
//!   daemon's pid namespace, which is the mount's: the daemon mounted it
//!   and the kernel reports the requesting task's id there; `EPERM` counts
//!   as existing, a recycled id as alive), and so does the sweep below.
//!   A task that exited with its reply uninstalled (killed while waiting)
//!   has no reply to protect: the kernel drops it.
//!
//! Replies to requests of pid 0 (a task outside the mount's pid namespace)
//! have no task id to ask or clear: one mark per inode stands for "some
//! such reply may be uninstalled" until a notice is written for the inode.
//!
//! **A reply may carry several inodes** (plan 40: a `READDIRPLUS` page
//! installs one dentry and inode per entry, all in the task's `getdents`).
//! [`AttrWatch::replying_each`] adds one inode to the set of marks the
//! task's current request has made so far, under the one mark id, instead
//! of replacing the task's mark as [`AttrWatch::replying`] does (one
//! inode: `GETATTR`, `LOOKUP`, `SETATTR`, a single entry). The set is
//! discharged as a whole by the task's next request, and by the inode: a
//! notice for one of its inodes releases that inode's mark and leaves the
//! others. A task's request is answered by one thread at a time, so the
//! set is built by one caller in a row; a pid-0 reply has no task to hold
//! them together and marks each inode on its own, as above.
//!
//! The table is bounded: [`MARKS_MAX`] marks (inodes) of tasks with an id
//! and as many for pid 0, separately, so a pid-0 client reading a large
//! tree cannot take the room of the others. It holds at most one mark per
//! live task id (a set of at most one page) and one per inode for pid 0,
//! so it reaches the bound only with that many inodes marked by tasks idle
//! after a reply, or a pid-0 task that read that many inodes since they
//! were last changed. A full table first sweeps the marks of exited tasks
//! (at most once per [`SWEEP_EVERY`], so a table full of live ones is not
//! scanned per reply); if it is still full:
//!
//! - a single-inode reply **writes its notice at once, before the reply**
//!   (the request was sent before the notice, so the kernel rejects
//!   exactly this reply), and no mark is needed. That costs a `GETATTR`
//!   per reply, never a stale attribute;
//! - a reply of several inodes gets [`Replying::Full`] and leaves that
//!   inode out (a `READDIRPLUS` entry goes name-only: the kernel looks the
//!   name up when it needs it). A notice per entry would be a storm, and
//!   each would also make the kernel reject the attributes the page
//!   installs. Nothing is installed for it, so nothing needs a mark.
//!
//! `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY=on` notices every operation.

use crate::refs::Mix;
use constellation_vfs::Ino;
use smallvec::SmallVec;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Marks held at most, of tasks with an id and (separately) of pid 0 (each
/// is a few dozen bytes in two maps: a few MiB).
pub(crate) const MARKS_MAX: usize = 32 * 1024;

/// A full table sweeps for exited tasks at most this often.
const SWEEP_EVERY: Duration = Duration::from_millis(100);

/// Changes remembered for the replies in flight. More than this many
/// namespace operations during one request counts as "changed".
const RING: usize = 256;

const SHARDS: usize = 16;

/// Task keys of requests without a pid have this bit set.
const ANONYMOUS: u64 = 1 << 63;

type Hash = BuildHasherDefault<Mix>;

/// A shard on a cache line of its own: neighbouring shards' locks are taken
/// by different threads at once.
#[derive(Default)]
#[repr(align(64))]
struct Padded<T>(T);

impl<T> std::ops::Deref for Padded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// The `(task key, mark id)` of an inode's marks: one allocation-free pair
/// in the usual case.
enum Chain {
    One(u64, u64),
    Many(HashMap<u64, u64, Hash>),
}

impl Chain {
    fn insert(&mut self, key: u64, id: u64) {
        match self {
            Self::One(k, i) if *k == key => *i = id,
            Self::One(k, i) => {
                let mut m = HashMap::default();
                m.insert(*k, *i);
                m.insert(key, id);
                *self = Self::Many(m);
            }
            Self::Many(m) if m.is_empty() => *self = Self::One(key, id),
            Self::Many(m) => {
                m.insert(key, id);
            }
        }
    }

    fn get(&self, key: u64) -> Option<u64> {
        match self {
            Self::One(k, i) => (*k == key).then_some(*i),
            Self::Many(m) => m.get(&key).copied(),
        }
    }

    fn remove(&mut self, key: u64) {
        match self {
            Self::One(k, _) if *k == key => *self = Self::Many(HashMap::default()),
            Self::One(..) => {}
            Self::Many(m) => {
                m.remove(&key);
                match m.len() {
                    // A drained chain gives its allocation back.
                    1 => {
                        if let Some((k, i)) = m.iter().next().map(|(k, i)| (*k, *i)) {
                            *self = Self::One(k, i);
                        }
                    }
                    n if m.capacity() > 4 * n + 16 => m.shrink_to_fit(),
                    _ => {}
                }
            }
        }
    }

    /// Whether a pid-0 mark is among them.
    fn has_anonymous(&self) -> bool {
        match self {
            Self::One(k, _) => anonymous(*k),
            Self::Many(m) => m.keys().any(|k| anonymous(*k)),
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Many(m) if m.is_empty())
    }

    fn entries(&self) -> Vec<(u64, u64)> {
        match self {
            Self::One(k, i) => vec![(*k, *i)],
            Self::Many(m) => m.iter().map(|(k, i)| (*k, *i)).collect(),
        }
    }
}

fn anonymous(key: u64) -> bool {
    key & ANONYMOUS != 0
}

/// Whether the task `tid` (a thread id in this process's pid namespace)
/// still exists.
fn task_exists(tid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(tid) else {
        return true;
    };
    // SAFETY: signal 0 only checks that the id exists and may be signalled.
    let rc = unsafe { libc::kill(pid, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// What a request saw when it arrived (see the module doc).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Arrival {
    /// `None`: nothing to track (the notice is not narrowed).
    track: Option<Tracked>,
}

#[derive(Debug, Clone, Copy)]
struct Tracked {
    tid: u32,
    seq: u64,
}

impl Arrival {
    /// A request that needs no tracking.
    pub(crate) const fn untracked() -> Self {
        Self { track: None }
    }
}

/// One mark: the inode a reply was for, and the id of the set it is in.
#[derive(Debug, Clone, Copy)]
struct Mark {
    ino: Ino,
    /// Unique per set: a removal names the mark it found, never a newer
    /// one of the same task.
    id: u64,
}

/// What a task's marks say: the inodes its current reply was for (one,
/// except for a reply of several inodes), under one id.
#[derive(Debug, Clone)]
struct TaskMarks {
    /// Never empty while the record is held.
    inos: SmallVec<[Ino; 1]>,
    id: u64,
}

impl TaskMarks {
    fn one(ino: Ino, id: u64) -> Self {
        Self {
            inos: SmallVec::from_buf([ino]),
            id,
        }
    }
}

/// What [`AttrWatch::record`] did to a task's record.
struct Recorded {
    /// The set's id the mark goes under.
    id: u64,
    /// The set a single-inode reply replaced.
    replaced: Option<TaskMarks>,
    /// Whether the inode is a new mark (to index). Not when a reply of
    /// several inodes names an inode its set already holds.
    fresh: bool,
}

/// What a reply of several inodes does for one of them.
#[derive(Debug)]
#[must_use = "a notice must be written and released; Full leaves the inode out"]
pub(crate) enum Replying {
    /// The inode is marked; nothing to write first.
    Clear,
    /// Write the notice for the inode before the reply, then release.
    Notice(Found),
    /// No room for another mark: leave the inode out of the reply.
    Full,
}

/// Marks a notice is being written for, `(key, ino, id)`: discharged by
/// [`AttrWatch::release`] once it is written.
#[must_use = "release the marks once the notice is written"]
#[derive(Debug, Default)]
pub(crate) struct Found(Vec<(u64, Ino, u64)>);

/// The inodes namespace operations changed, newest last.
#[derive(Default)]
struct Changes {
    seq: AtomicU64,
    ring: Mutex<Ring>,
}

#[derive(Default)]
struct Ring {
    /// `(sequence number, inodes)`.
    entries: VecDeque<(u64, [Ino; 4], usize)>,
    /// The newest sequence number dropped from `entries`.
    dropped: u64,
}

impl Changes {
    fn record(&self, inos: &[Ino]) {
        let mut ring = self.ring.lock().unwrap();
        let seq = self.seq.load(Ordering::Relaxed) + 1;
        let mut list = [0; 4];
        let n = inos.len().min(list.len());
        list[..n].copy_from_slice(&inos[..n]);
        ring.entries.push_back((seq, list, n));
        while ring.entries.len() > RING {
            if let Some((old, ..)) = ring.entries.pop_front() {
                ring.dropped = old;
            }
        }
        self.seq.store(seq, Ordering::Release);
    }

    /// Whether `ino` may have been changed after sequence number `since`.
    fn touched_since(&self, since: u64, ino: Ino) -> bool {
        let ring = self.ring.lock().unwrap();
        ring.dropped > since
            || ring
                .entries
                .iter()
                .any(|(seq, inos, n)| *seq > since && inos[..*n].contains(&ino))
    }
}

/// The session's record of attribute replies in flight.
///
/// Two maps over the same marks: by task (to clear a task's marks when its
/// next request arrives) and by inode (to find the marks of a changed inode
/// in one lookup). `inodes` is what a mark *is*: a notice finds a mark
/// there. `tasks` holds a task's marks, for the clearing, the count, and
/// the bound, and is a superset: a mark goes in `tasks` first and out of it
/// last, so a key in `inodes` always has its task record (the invariant
/// the tests check). No operation holds both locks.
pub(crate) struct AttrWatch {
    changes: Changes,
    /// By task key (task id, or an anonymous key).
    tasks: Vec<Padded<Mutex<HashMap<u64, TaskMarks, Hash>>>>,
    /// By inode: the `(task key, mark id)` of its marks.
    inodes: Vec<Padded<Mutex<HashMap<Ino, Chain, Hash>>>>,
    /// Keys for the marks of requests without a pid.
    anonymous: AtomicU64,
    /// [`TaskMarks::id`]s.
    ids: AtomicU64,
    /// Marks (inodes) held: of tasks with an id, and of pid 0. A change
    /// finds no mark without taking a lock when both are 0 (a single
    /// task's reply is installed before its next request, so its own
    /// changes find none).
    held: [AtomicUsize; 2],
    /// The bound on each.
    max: usize,
    /// When the table was last swept for exited tasks.
    swept: Mutex<Option<Instant>>,
    /// Times a reply found the table full (it wrote its notice at once, or
    /// left the inode out).
    full: AtomicU64,
    /// Whether a task id still exists (`task_exists`; replaced in tests).
    exists: Box<dyn Fn(u32) -> bool + Send + Sync>,
}

impl Default for AttrWatch {
    fn default() -> Self {
        Self::with(MARKS_MAX, Box::new(task_exists))
    }
}

impl AttrWatch {
    fn with(max: usize, exists: Box<dyn Fn(u32) -> bool + Send + Sync>) -> Self {
        Self {
            changes: Changes::default(),
            tasks: (0..SHARDS).map(|_| Padded::default()).collect(),
            inodes: (0..SHARDS).map(|_| Padded::default()).collect(),
            anonymous: AtomicU64::new(0),
            ids: AtomicU64::new(0),
            held: [AtomicUsize::new(0), AtomicUsize::new(0)],
            max,
            swept: Mutex::new(None),
            full: AtomicU64::new(0),
            exists,
        }
    }

    fn task_shard(&self, key: u64) -> &Mutex<HashMap<u64, TaskMarks, Hash>> {
        &self.tasks[key as usize % SHARDS]
    }

    fn inode_shard(&self, ino: Ino) -> &Mutex<HashMap<Ino, Chain, Hash>> {
        &self.inodes[ino as usize % SHARDS]
    }

    /// An attribute-carrying or namespace request of task `tid` arrives:
    /// whatever reply it was waiting for is installed. Returns what the
    /// request must remember until it replies.
    pub(crate) fn arrive(&self, tid: u32) -> Arrival {
        if tid != 0 {
            let key = u64::from(tid);
            let task = self.task_shard(key).lock().unwrap().remove(&key);
            if let Some(task) = task {
                self.held[0].fetch_sub(task.inos.len(), Ordering::Relaxed);
                for &ino in &task.inos {
                    self.unindex(key, Mark { ino, id: task.id });
                }
            }
        }
        Arrival {
            track: Some(Tracked {
                tid,
                seq: self.changes.seq.load(Ordering::Acquire),
            }),
        }
    }

    /// The request replies with the attributes of `ino` (and with no
    /// other inode's: the task's mark is this one). `Some` when the reply
    /// must be preceded by a notice for `ino`: a change of it was recorded
    /// since the request arrived, or the table of marks is full. The marks
    /// of `ino`, to [`Self::release`] once the notice is written.
    pub(crate) fn replying(&self, a: Arrival, ino: Ino) -> Option<Found> {
        match self.reply(a, ino, false) {
            Replying::Clear => None,
            Replying::Notice(found) => Some(found),
            // The notice discharges the inode's marks too: room for the
            // next reply.
            Replying::Full => Some(self.find(&[ino]).1),
        }
    }

    /// The request replies with the attributes of several inodes, one call
    /// each, all installed by the task's next syscall: the call **adds**
    /// `ino` to the set of marks this request has made (module doc),
    /// where [`Self::replying`] makes it the task's only mark.
    /// [`Replying::Notice`]: the same as `replying`'s `Some`.
    /// [`Replying::Full`]: no room and none could be made; nothing was
    /// marked and nothing is written, the caller leaves the inode out.
    // Plan 40 P6 (`READDIRPLUS`) is the caller.
    #[allow(dead_code)]
    pub(crate) fn replying_each(&self, a: Arrival, ino: Ino) -> Replying {
        self.reply(a, ino, true)
    }

    fn reply(&self, a: Arrival, ino: Ino, each: bool) -> Replying {
        let Some(t) = a.track else {
            return Replying::Clear;
        };
        if !self.mark(t.tid, ino, each) {
            self.full.fetch_add(1, Ordering::Relaxed);
            return Replying::Full;
        }
        fence(Ordering::SeqCst);
        if self.changes.seq.load(Ordering::Acquire) == t.seq {
            return Replying::Clear;
        }
        if self.changes.touched_since(t.seq, ino) {
            return Replying::Notice(self.find(&[ino]).1);
        }
        Replying::Clear
    }

    /// Records the reply of `tid` for `ino` (`each`: in the set of the
    /// task's current request); `false` when the table is full and
    /// sweeping exited tasks did not make room.
    fn mark(&self, tid: u32, ino: Ino, each: bool) -> bool {
        let key = if tid == 0 {
            ANONYMOUS | self.anonymous.fetch_add(1, Ordering::Relaxed)
        } else {
            u64::from(tid)
        };
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        // A pid-0 reply of an inode that has a pid-0 mark replaces it (the
        // mark below supersedes it): it needs no room, a re-read of a tree
        // the table is full of costs no notice.
        let over = anonymous(key)
            && self
                .inode_shard(ino)
                .lock()
                .unwrap()
                .get(&ino)
                .is_some_and(Chain::has_anonymous);
        let Some(rec) = self.record(key, ino, id, each, over).or_else(|| {
            (!anonymous(key) && self.sweep())
                .then(|| self.record(key, ino, id, each, false))
                .flatten()
        }) else {
            return false;
        };
        let mark = Mark { ino, id: rec.id };
        // After the task record (see the type's doc), before the fence.
        let superseded = if rec.fresh {
            self.index(key, mark)
        } else {
            Vec::new()
        };
        if over && superseded.is_empty() && self.held[1].load(Ordering::Relaxed) > self.max {
            // The mark it meant to replace went meanwhile: no room after
            // all.
            self.discharge(key, mark);
            return false;
        }
        if let Some(old) = rec.replaced {
            for &i in &old.inos {
                self.unindex(key, Mark { ino: i, id: old.id });
            }
        }
        // An older reply of this inode to a pid-0 task is covered by the
        // notice this mark will get: one mark per inode stands for both.
        for (k, id) in superseded {
            self.discharge(k, Mark { ino, id });
        }
        true
    }

    /// Stores the mark in `key`'s record. `each`: in the record's set when
    /// it has one (a set the inode is in already takes no room), else a
    /// new record of one; without it the record is replaced by one of this
    /// inode alone. `None` when a new mark does not fit (`over`: it may
    /// exceed the bound by one, a mark it supersedes is about to go).
    fn record(&self, key: u64, ino: Ino, id: u64, each: bool, over: bool) -> Option<Recorded> {
        let held = &self.held[usize::from(anonymous(key))];
        let mut shard = self.task_shard(key).lock().unwrap();
        match shard.entry(key) {
            Entry::Occupied(mut e) if each => {
                let marks = e.get_mut();
                if marks.inos.contains(&ino) {
                    return Some(Recorded {
                        id: marks.id,
                        replaced: None,
                        fresh: false,
                    });
                }
                if held.fetch_add(1, Ordering::Relaxed) >= self.max && !over {
                    held.fetch_sub(1, Ordering::Relaxed);
                    return None;
                }
                marks.inos.push(ino);
                Some(Recorded {
                    id: marks.id,
                    replaced: None,
                    fresh: true,
                })
            }
            Entry::Occupied(mut e) => {
                // One mark replaces the set: the count only goes down.
                let old = e.insert(TaskMarks::one(ino, id));
                if old.inos.len() > 1 {
                    held.fetch_sub(old.inos.len() - 1, Ordering::Relaxed);
                }
                Some(Recorded {
                    id,
                    replaced: Some(old),
                    fresh: true,
                })
            }
            Entry::Vacant(e) => {
                if held.fetch_add(1, Ordering::Relaxed) >= self.max && !over {
                    held.fetch_sub(1, Ordering::Relaxed);
                    return None;
                }
                e.insert(TaskMarks::one(ino, id));
                Some(Recorded {
                    id,
                    replaced: None,
                    fresh: true,
                })
            }
        }
    }

    /// Adds the mark to its inode's index. Returns the pid-0 marks of the
    /// inode it supersedes.
    fn index(&self, key: u64, mark: Mark) -> Vec<(u64, u64)> {
        let mut shard = self.inode_shard(mark.ino).lock().unwrap();
        let mut superseded = Vec::new();
        match shard.entry(mark.ino) {
            Entry::Vacant(e) => {
                e.insert(Chain::One(key, mark.id));
            }
            Entry::Occupied(mut e) => {
                let chain = e.get_mut();
                if anonymous(key) {
                    superseded = chain
                        .entries()
                        .into_iter()
                        .filter(|(k, _)| anonymous(*k))
                        .collect();
                    for (k, _) in &superseded {
                        chain.remove(*k);
                    }
                }
                chain.insert(key, mark.id);
            }
        }
        superseded
    }

    /// Takes the mark out of its inode's index (if that mark is still the
    /// one there).
    fn unindex(&self, key: u64, mark: Mark) {
        let mut shard = self.inode_shard(mark.ino).lock().unwrap();
        if let Entry::Occupied(mut e) = shard.entry(mark.ino) {
            if e.get().get(key) == Some(mark.id) {
                e.get_mut().remove(key);
            }
            if e.get().is_empty() {
                e.remove();
            }
        }
    }

    /// Drops the mark: out of the index first, then out of the task's
    /// record (only that mark: a newer set of the task stays, and so do
    /// the other inodes of this one).
    fn discharge(&self, key: u64, mark: Mark) {
        self.unindex(key, mark);
        let mut shard = self.task_shard(key).lock().unwrap();
        if let Entry::Occupied(mut e) = shard.entry(key) {
            let marks = e.get_mut();
            if marks.id != mark.id {
                return;
            }
            let Some(at) = marks.inos.iter().position(|i| *i == mark.ino) else {
                return;
            };
            marks.inos.swap_remove(at);
            self.held[usize::from(anonymous(key))].fetch_sub(1, Ordering::Relaxed);
            if marks.inos.is_empty() {
                e.remove();
            }
        }
    }

    /// Drops the marks of tasks that no longer exist (at most once per
    /// [`SWEEP_EVERY`]). `true` when it made room.
    fn sweep(&self) -> bool {
        let Ok(mut swept) = self.swept.try_lock() else {
            return false;
        };
        let now = Instant::now();
        if swept.is_some_and(|t| now.duration_since(t) < SWEEP_EVERY) {
            return false;
        }
        let mut freed = 0;
        for shard in &self.tasks {
            // The probes run outside the lock: a shard holds thousands.
            let tasks: Vec<(u64, TaskMarks)> = shard
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| !anonymous(**k))
                .map(|(k, t)| (*k, t.clone()))
                .collect();
            for (key, task) in tasks {
                if !(self.exists)(key as u32) {
                    for &ino in &task.inos {
                        self.discharge(key, Mark { ino, id: task.id });
                        freed += 1;
                    }
                }
            }
        }
        *swept = Some(Instant::now());
        freed > 0
    }

    /// A namespace operation changed `inos`: record them, and return those
    /// a reply may still be on its way for, with their marks. The caller
    /// writes a notice for each and then [`Self::release`]s the marks: the
    /// notice rejects every reply sent before it.
    pub(crate) fn changed(&self, inos: &[Ino]) -> (Vec<Ino>, Found) {
        self.changes.record(inos);
        fence(Ordering::SeqCst);
        self.find(inos)
    }

    /// The marks of `inos` whose task still exists (dropping those that
    /// do not): the inodes that have one, and the marks. They stay until
    /// [`Self::release`], so a concurrent change of the same inode still
    /// finds them.
    fn find(&self, inos: &[Ino]) -> (Vec<Ino>, Found) {
        let mut found = Vec::new();
        let mut marks = Found::default();
        if self.held() == 0 {
            return (found, marks);
        }
        for &ino in inos {
            if found.contains(&ino) {
                continue;
            }
            let chain: Vec<(u64, u64)> = self
                .inode_shard(ino)
                .lock()
                .unwrap()
                .get(&ino)
                .map(Chain::entries)
                .unwrap_or_default();
            for (key, id) in chain {
                if !anonymous(key) && !(self.exists)(key as u32) {
                    self.discharge(key, Mark { ino, id });
                    continue;
                }
                if !found.contains(&ino) {
                    found.push(ino);
                }
                marks.0.push((key, ino, id));
            }
        }
        (found, marks)
    }

    /// The notice for the marks of `found` is written: discharge them (a
    /// mark the task's next request cleared, or a newer reply of the task
    /// replaced, is left alone).
    pub(crate) fn release(&self, found: Found) {
        for (key, ino, id) in found.0 {
            self.discharge(key, Mark { ino, id });
        }
    }

    /// Marks (inodes) held.
    fn held(&self) -> usize {
        self.held[0].load(Ordering::Relaxed) + self.held[1].load(Ordering::Relaxed)
    }

    /// Marks held, counted in the records (tests, `node.status` has no use
    /// for it).
    #[cfg(test)]
    fn marks(&self) -> usize {
        self.tasks
            .iter()
            .map(|s| {
                s.lock()
                    .unwrap()
                    .values()
                    .map(|t| t.inos.len())
                    .sum::<usize>()
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    /// A watch whose tasks all exist.
    fn watch() -> AttrWatch {
        AttrWatch::with(MARKS_MAX, Box::new(|_| true))
    }

    /// A watch of at most `max` marks and the set of task ids that no
    /// longer exist.
    fn watch_of(max: usize) -> (AttrWatch, Arc<Mutex<HashSet<u32>>>) {
        let gone = Arc::new(Mutex::new(HashSet::new()));
        let g = gone.clone();
        let w = AttrWatch::with(max, Box::new(move |t| !g.lock().unwrap().contains(&t)));
        (w, gone)
    }

    /// The two maps agree: every indexed mark has its task record, and the
    /// count is the number of marks in the records.
    fn check(w: &AttrWatch) {
        let tasks: HashMap<u64, TaskMarks> = w
            .tasks
            .iter()
            .flat_map(|s| {
                s.lock()
                    .unwrap()
                    .iter()
                    .map(|(k, t)| (*k, t.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        for t in tasks.values() {
            assert!(!t.inos.is_empty(), "an empty record is dropped");
            let distinct: HashSet<_> = t.inos.iter().collect();
            assert_eq!(distinct.len(), t.inos.len(), "an inode once per set");
        }
        let count = |anon: bool| -> usize {
            tasks
                .iter()
                .filter(|(k, _)| anonymous(**k) == anon)
                .map(|(_, t)| t.inos.len())
                .sum()
        };
        let (anon, named) = (count(true), count(false));
        assert_eq!(anon + named, w.held(), "count");
        assert_eq!(anon, w.held[1].load(Ordering::Relaxed), "pid-0 count");
        assert!(anon <= w.max && named <= w.max, "bound");
        for (i, s) in w.inodes.iter().enumerate() {
            for (ino, chain) in s.lock().unwrap().iter() {
                assert_eq!(*ino as usize % SHARDS, i, "shard");
                assert!(!chain.is_empty(), "an empty chain is dropped");
                for (key, id) in chain.entries() {
                    let t = tasks.get(&key).expect("an indexed mark has its record");
                    assert!(
                        t.inos.contains(ino) && t.id == id,
                        "the record holds that mark"
                    );
                }
            }
        }
    }

    /// A change whose notices are written at once.
    fn change(w: &AttrWatch, inos: &[Ino]) -> Vec<Ino> {
        let (found, marks) = w.changed(inos);
        w.release(marks);
        found
    }

    /// A reply whose notice, if any, is written at once.
    fn reply(w: &AttrWatch, a: Arrival, ino: Ino) -> bool {
        w.replying(a, ino).map(|m| w.release(m)).is_some()
    }

    /// What one inode of a reply of several came to.
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum Each {
        Marked,
        Noticed,
        Full,
    }

    /// One inode of a reply of several, its notice, if any, written at
    /// once.
    fn each(w: &AttrWatch, a: Arrival, ino: Ino) -> Each {
        match w.replying_each(a, ino) {
            Replying::Clear => Each::Marked,
            Replying::Notice(m) => {
                w.release(m);
                Each::Noticed
            }
            Replying::Full => Each::Full,
        }
    }

    #[test]
    fn a_reply_not_yet_followed_by_a_request_is_a_mark_a_change_finds() {
        let w = watch();
        let a = w.arrive(7);
        assert!(!reply(&w, a, 100));
        assert_eq!(w.marks(), 1);
        check(&w);
        assert_eq!(change(&w, &[100, 5]), vec![100]);
        // discharged: the next change finds nothing
        assert_eq!(change(&w, &[100]), Vec::<Ino>::new());
        assert_eq!(w.marks(), 0);
        check(&w);
    }

    #[test]
    fn the_tasks_next_request_clears_its_mark() {
        let w = watch();
        let a = w.arrive(7);
        reply(&w, a, 100);
        w.arrive(7);
        assert_eq!(w.marks(), 0);
        check(&w);
        assert_eq!(change(&w, &[100]), Vec::<Ino>::new());
    }

    #[test]
    fn another_tasks_request_clears_nothing_of_this_one() {
        let w = watch();
        let a = w.arrive(7);
        reply(&w, a, 100);
        w.arrive(8);
        assert_eq!(change(&w, &[100]), vec![100]);
    }

    #[test]
    fn a_change_during_a_request_makes_its_reply_write_the_notice() {
        let w = watch();
        let a = w.arrive(7);
        // The change completes (its own check finds no mark) before the
        // reply is written.
        assert!(change(&w, &[100]).is_empty());
        assert!(reply(&w, a, 100));
        // Another inode's reply is not affected.
        let b = w.arrive(8);
        assert!(change(&w, &[5]).is_empty());
        assert!(!reply(&w, b, 100));
    }

    #[test]
    fn a_change_before_the_request_arrived_is_not_its_business() {
        let w = watch();
        change(&w, &[100]);
        let a = w.arrive(7);
        assert!(!reply(&w, a, 100));
    }

    #[test]
    fn a_ring_overflow_counts_as_a_change() {
        let w = watch();
        let a = w.arrive(7);
        for i in 0..(RING as u64 + 8) {
            change(&w, &[1000 + i]);
        }
        assert!(reply(&w, a, 100));
    }

    #[test]
    fn the_count_follows_the_marks() {
        let w = watch();
        let count = |w: &AttrWatch| w.held();
        for tid in 1..=5 {
            let a = w.arrive(tid);
            reply(&w, a, 100 + u64::from(tid));
        }
        assert_eq!((count(&w), w.marks()), (5, 5));
        check(&w);
        // A second reply of a task replaces its mark.
        let a = w.arrive(1);
        reply(&w, a, 200);
        assert_eq!((count(&w), w.marks()), (5, 5));
        w.arrive(2);
        assert_eq!((count(&w), w.marks()), (4, 4));
        let mut found = change(&w, &[103, 200]);
        found.sort_unstable();
        assert_eq!(found, vec![103, 200]);
        assert_eq!((count(&w), w.marks()), (2, 2));
        check(&w);
        assert!(change(&w, &[999]).is_empty());
        // Nothing left to find: a change takes no lock and finds nothing.
        w.arrive(4);
        w.arrive(5);
        assert_eq!((count(&w), w.marks()), (0, 0));
        assert!(change(&w, &[1]).is_empty());
        check(&w);
    }

    #[test]
    fn an_untracked_request_leaves_no_mark() {
        let w = watch();
        assert!(!reply(&w, Arrival::untracked(), 100));
        assert_eq!(w.marks(), 0);
    }

    /// Plan 42 F2 of review: a reply the kernel installs more than a
    /// second after it was generated (a ring thread behind a slow handler,
    /// a task a throttled cgroup kept off its CPU) is still covered: a
    /// mark is not a clock. (Failed when marks expired after a second.)
    #[test]
    fn a_reply_installed_more_than_a_second_later_is_still_covered() {
        let w = watch();
        let a = w.arrive(7);
        assert!(!reply(&w, a, 100));
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(change(&w, &[100]), vec![100], "not installed yet");
        // The notice discharged it.
        assert!(change(&w, &[100]).is_empty());
        // And the same when the task's next request is what ends the
        // wait, however late.
        let a = w.arrive(8);
        assert!(!reply(&w, a, 101));
        std::thread::sleep(Duration::from_millis(1100));
        w.arrive(8);
        assert!(change(&w, &[101]).is_empty(), "installed: the task went on");
        check(&w);
    }

    /// A change that found a mark has not written its notice yet: a
    /// second change of the inode still finds the mark, and writes its own
    /// notice before its reply (the first one's may come later).
    #[test]
    fn a_mark_stays_until_the_notice_that_found_it_is_written() {
        let w = watch();
        let a = w.arrive(7);
        assert!(!reply(&w, a, 100));
        let (first, marks) = w.changed(&[100]);
        assert_eq!(first, vec![100]);
        assert_eq!(
            change(&w, &[100]),
            vec![100],
            "the first notice is not written yet"
        );
        w.release(marks);
        assert_eq!(w.marks(), 0);
        assert!(change(&w, &[100]).is_empty());
        check(&w);
    }

    /// The same for a reply that writes the notice itself: until it has,
    /// the other replies' marks of the inode stay for a change to find.
    #[test]
    fn a_late_replys_notice_discharges_marks_only_once_written() {
        let w = watch();
        let late = w.arrive(8);
        assert!(change(&w, &[100]).is_empty());
        let other = w.arrive(7);
        assert!(!reply(&w, other, 100));
        let marks = w.replying(late, 100).expect("changed since it arrived");
        assert_eq!(
            change(&w, &[100]),
            vec![100],
            "the late reply's notice is not written yet"
        );
        w.release(marks);
        assert_eq!(w.marks(), 0);
        check(&w);
    }

    /// A release leaves a newer mark of the same task alone.
    #[test]
    fn a_release_spares_a_newer_mark_of_the_task() {
        let w = watch();
        let a = w.arrive(7);
        reply(&w, a, 100);
        let (_, marks) = w.changed(&[100]);
        // The task installed, sent its next request and got another reply.
        let a = w.arrive(7);
        reply(&w, a, 100);
        w.release(marks);
        assert_eq!(w.marks(), 1);
        check(&w);
        assert_eq!(change(&w, &[100]), vec![100]);
        assert_eq!(w.held(), 0);
    }

    /// Evidence 3: the task is gone. A change that finds its mark asks, and
    /// drops the mark instead of writing a notice for it.
    #[test]
    fn the_mark_of_an_exited_task_is_dropped_when_a_change_finds_it() {
        let (w, gone) = watch_of(MARKS_MAX);
        for tid in [7, 8] {
            let a = w.arrive(tid);
            reply(&w, a, 100);
        }
        let a = w.arrive(9);
        reply(&w, a, 101);
        gone.lock().unwrap().extend([7, 9]);
        // 8 is alive: the inode is noticed; 7 is gone: dropped.
        assert_eq!(change(&w, &[100]), vec![100]);
        assert_eq!(w.marks(), 1, "7 dropped, 8 released, 9 not looked at");
        check(&w);
        // Only a gone task's mark: nothing to notice.
        assert!(change(&w, &[101]).is_empty());
        assert_eq!(w.marks(), 0);
        check(&w);
    }

    /// A pid-0 reply has no task to clear or ask: one mark per inode,
    /// until a notice for the inode, however long it takes.
    #[test]
    fn a_reply_to_an_unknown_task_stays_until_its_inode_is_noticed() {
        let (w, _) = watch_of(MARKS_MAX);
        let a = w.arrive(0);
        reply(&w, a, 100);
        // Its "next request" clears nothing.
        w.arrive(0);
        assert_eq!(w.marks(), 1);
        // More replies for the inode share the one mark.
        for _ in 0..100 {
            let a = w.arrive(0);
            reply(&w, a, 100);
        }
        assert_eq!(w.marks(), 1, "one mark per inode");
        check(&w);
        assert_eq!(change(&w, &[100]), vec![100]);
        assert_eq!(w.marks(), 0);
        check(&w);
        // Marks for other inodes are independent.
        for ino in 1..=10 {
            let a = w.arrive(0);
            reply(&w, a, ino);
        }
        assert_eq!(w.marks(), 10);
        assert_eq!(change(&w, &[3, 4]), vec![3, 4]);
        assert_eq!(w.marks(), 8);
        check(&w);
    }

    /// The table never holds more than its bound. A full table writes the
    /// notice before the reply rather than keep a mark it has no room for.
    #[test]
    fn a_full_table_writes_the_notice_at_once_and_holds_no_more() {
        let (w, _) = watch_of(4);
        for tid in 1..=4 {
            let a = w.arrive(tid);
            assert!(!reply(&w, a, 100 + u64::from(tid)));
        }
        assert_eq!(w.marks(), 4);
        // A fifth task: no room (all four alive).
        let a = w.arrive(5);
        assert!(reply(&w, a, 105), "the notice goes before this reply");
        assert_eq!((w.marks(), w.full.load(Ordering::Relaxed)), (4, 1));
        // A task that already has a mark replaces it: no new room needed.
        let a = w.arrive(1);
        assert!(!reply(&w, a, 111));
        assert_eq!(w.marks(), 4);
        // pid 0 has room of its own: a pid-0 client reading a large tree
        // does not take the others'.
        for ino in 120..124 {
            let a = w.arrive(0);
            assert!(!reply(&w, a, ino));
        }
        assert_eq!(w.marks(), 8);
        let a = w.arrive(0);
        assert!(reply(&w, a, 124), "the pid-0 room is full");
        assert_eq!(w.marks(), 8);
        check(&w);
        // Room again once a task has sent its next request.
        w.arrive(2);
        let a = w.arrive(6);
        assert!(!reply(&w, a, 106));
        assert_eq!(w.marks(), 8);
        // And for pid 0 once its inode was noticed.
        assert_eq!(change(&w, &[120]), vec![120]);
        let a = w.arrive(0);
        assert!(!reply(&w, a, 125));
        check(&w);
    }

    /// The fallback's notice discharges the inode's marks: it makes room.
    #[test]
    fn the_full_tables_notice_releases_the_inodes_marks() {
        let (w, _) = watch_of(2);
        for (tid, ino) in [(1, 100), (2, 101)] {
            let a = w.arrive(tid);
            assert!(!reply(&w, a, ino));
        }
        let a = w.arrive(3);
        assert!(reply(&w, a, 100), "no room: noticed");
        assert_eq!(w.marks(), 1, "the notice released the mark of 100");
        let a = w.arrive(4);
        assert!(!reply(&w, a, 102), "room again");
        check(&w);
    }

    /// A pid-0 walk over more inodes than the table holds stays within it,
    /// notices only the replies that found no room, re-reads of the marked
    /// ones cost no notice, and the table frees as inodes are noticed.
    #[test]
    fn a_pid_0_walk_past_the_bound_neither_grows_nor_stays_full() {
        let (w, _) = watch_of(8);
        let walk = |inos: std::ops::RangeInclusive<u64>| -> usize {
            inos.filter(|&ino| {
                let a = w.arrive(0);
                reply(&w, a, ino)
            })
            .count()
        };
        assert_eq!(walk(1..=20), 12, "the first 8 are marked, the rest noticed");
        assert_eq!(w.marks(), 8);
        for _ in 0..3 {
            assert_eq!(walk(1..=8), 0, "a marked inode's mark is reused");
            assert_eq!(w.marks(), 8);
        }
        assert_eq!(walk(9..=12), 4, "no room for others");
        check(&w);
        // Changes notice the marked inodes and free their marks.
        assert_eq!(change(&w, &[1, 2, 3, 4]).len(), 4);
        assert_eq!(walk(9..=12), 0, "room again: no notice per reply");
        assert_eq!(w.marks(), 8);
        check(&w);
    }

    /// A drained chain gives its allocation back.
    #[test]
    fn a_drained_chain_collapses_and_shrinks() {
        let mut c = Chain::One(1, 10);
        for k in 2..=1000 {
            c.insert(k, k * 10);
        }
        assert_eq!(c.entries().len(), 1000);
        for k in 11..=1000 {
            c.remove(k);
        }
        match &c {
            Chain::Many(m) => assert!(m.capacity() <= 4 * m.len() + 16, "{}", m.capacity()),
            Chain::One(..) => panic!("ten entries left"),
        }
        for k in 2..=10 {
            c.remove(k);
        }
        assert!(matches!(c, Chain::One(1, 10)), "one entry left");
        c.remove(1);
        assert!(c.is_empty());
        c.insert(5, 50);
        assert!(matches!(c, Chain::One(5, 50)));
    }

    /// A full table drops the marks of exited tasks before it gives up, and
    /// not more often than every `SWEEP_EVERY`.
    #[test]
    fn a_full_table_sweeps_exited_tasks_at_a_bounded_rate() {
        let (w, gone) = watch_of(4);
        let probes = Arc::new(AtomicUsize::new(0));
        let p = probes.clone();
        let g = gone.clone();
        let w = AttrWatch {
            exists: Box::new(move |t| {
                p.fetch_add(1, Ordering::Relaxed);
                !g.lock().unwrap().contains(&t)
            }),
            ..w
        };
        for tid in 1..=4 {
            let a = w.arrive(tid);
            reply(&w, a, 100 + u64::from(tid));
        }
        gone.lock().unwrap().extend([1, 2]);
        let a = w.arrive(5);
        assert!(!reply(&w, a, 105), "room made by the sweep");
        assert_eq!(w.marks(), 3, "two swept, one added");
        assert_eq!(probes.load(Ordering::Relaxed), 4, "each task asked once");
        check(&w);
        // Full again (4 live + this one), within the interval: no sweep.
        let a = w.arrive(6);
        assert!(!reply(&w, a, 106));
        gone.lock().unwrap().insert(3);
        let a = w.arrive(7);
        assert!(reply(&w, a, 107), "no room, and swept a moment ago");
        assert_eq!(probes.load(Ordering::Relaxed), 4, "not swept again");
        std::thread::sleep(SWEEP_EVERY + Duration::from_millis(20));
        let a = w.arrive(8);
        assert!(!reply(&w, a, 108), "swept again: task 3 is gone");
        assert!(probes.load(Ordering::Relaxed) > 4);
        check(&w);
    }

    /// The bound holds for the real size, for task ids and for pid 0, and
    /// the shards stay consistent.
    #[test]
    fn the_table_is_bounded() {
        let w = watch();
        let mut noticed = 0;
        for tid in 1..=(2 * MARKS_MAX as u32) {
            let a = w.arrive(tid);
            noticed += usize::from(reply(&w, a, u64::from(tid)));
        }
        assert_eq!(w.marks(), MARKS_MAX);
        assert_eq!(noticed, MARKS_MAX, "every reply past the bound noticed");
        check(&w);
        // pid 0: one mark per inode, so many inodes fill it too.
        let w = watch();
        for ino in 1..=(2 * MARKS_MAX as u64) {
            let a = w.arrive(0);
            reply(&w, a, ino);
        }
        assert_eq!(w.marks(), MARKS_MAX);
        check(&w);
    }

    /// A model-free stress of the two maps: a mix of replies, requests,
    /// changes, releases and exits from several threads leaves them
    /// consistent and the table empty once every task has gone on.
    #[test]
    fn the_index_and_the_task_records_stay_consistent_under_threads() {
        let (w, gone) = watch_of(64);
        let w = Arc::new(w);
        let handles: Vec<_> = (0..4u32)
            .map(|t| {
                let (w, gone) = (w.clone(), gone.clone());
                std::thread::spawn(move || {
                    let mut x = 0x9e37_79b9_u64 + u64::from(t);
                    for step in 0..20_000u64 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let tid = (x % 40) as u32 + 1;
                        let ino = (x >> 8) % 16;
                        match (x >> 20) % 7 {
                            0..=2 => {
                                let a = w.arrive(tid * 4 + t % 4);
                                reply(&w, a, ino);
                            }
                            3 => {
                                w.arrive(tid * 4 + t % 4);
                            }
                            4 => {
                                change(&w, &[ino, (ino + 1) % 16]);
                            }
                            5 => {
                                let a = w.arrive(0);
                                reply(&w, a, ino);
                            }
                            _ => {
                                let mut g = gone.lock().unwrap();
                                if step % 2 == 0 {
                                    g.insert(tid * 4 + t % 4);
                                } else {
                                    g.remove(&(tid * 4 + t % 4));
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        check(&w);
        gone.lock().unwrap().clear();
        for tid in 1..=200 {
            w.arrive(tid);
        }
        for ino in 0..17 {
            change(&w, &[ino]);
        }
        assert_eq!(w.marks(), 0, "every task went on, every inode noticed");
        check(&w);
    }

    /// Both orders of a reply against a change, from two threads: the
    /// notice is always written by one of them (pins the protocol; the
    /// fences matter on weak-memory hosts, which this does not exercise).
    #[test]
    fn a_reply_and_a_change_never_both_miss_each_other() {
        use std::sync::Barrier;
        let w = Arc::new(watch());
        let missed = Arc::new(AtomicUsize::new(0));
        for round in 0..2000u64 {
            let ino = 10_000 + round;
            let go = Arc::new(Barrier::new(2));
            let (w1, go1) = (w.clone(), go.clone());
            let reader = std::thread::spawn(move || {
                let a = w1.arrive(1);
                go1.wait();
                reply(&w1, a, ino)
            });
            go.wait();
            let found = change(&w, &[ino]);
            let wrote = reader.join().unwrap();
            // The change happened after the reader arrived (it arrived
            // before the barrier): somebody must write the notice.
            if !wrote && found.is_empty() {
                missed.fetch_add(1, Ordering::Relaxed);
            }
        }
        assert_eq!(missed.load(Ordering::Relaxed), 0);
    }

    /// Plan 40 P6a: a reply of several inodes (a `READDIRPLUS` page) keeps
    /// every one marked, so a change of the first still finds its mark.
    /// (Failed when a task kept one mark: the last entry's.)
    #[test]
    fn a_reply_of_many_inodes_keeps_every_mark() {
        let w = watch();
        let a = w.arrive(7);
        for ino in [100, 101, 102] {
            assert_eq!(each(&w, a, ino), Each::Marked);
        }
        assert_eq!((w.marks(), w.held()), (3, 3));
        check(&w);
        // The first entry's inode changes: its mark is there.
        assert_eq!(change(&w, &[100]), vec![100]);
        assert_eq!(w.marks(), 2, "only that inode's mark is discharged");
        check(&w);
        let mut found = change(&w, &[101, 102, 103]);
        found.sort_unstable();
        assert_eq!(found, vec![101, 102]);
        assert_eq!(w.marks(), 0);
        check(&w);
    }

    /// The contrast: a single-inode reply per entry (what a page would
    /// do without `replying_each`) leaves the last one marked.
    #[test]
    fn a_single_inode_reply_per_entry_keeps_only_the_last_mark() {
        let w = watch();
        let a = w.arrive(7);
        for ino in [100, 101, 102] {
            assert!(!reply(&w, a, ino));
        }
        assert_eq!(w.marks(), 1);
        assert!(change(&w, &[100]).is_empty(), "race 2b would be open");
        assert_eq!(change(&w, &[102]), vec![102]);
    }

    #[test]
    fn the_tasks_next_request_discharges_the_whole_set() {
        let w = watch();
        let a = w.arrive(7);
        for ino in [100, 101, 102] {
            each(&w, a, ino);
        }
        let b = w.arrive(8);
        for ino in [200, 201] {
            each(&w, b, ino);
        }
        assert_eq!(w.marks(), 5);
        // Another task's request leaves the set alone.
        w.arrive(9);
        assert_eq!(w.marks(), 5);
        w.arrive(7);
        assert_eq!((w.marks(), w.held()), (2, 2));
        check(&w);
        assert!(change(&w, &[100, 101, 102]).is_empty(), "installed");
        assert_eq!(change(&w, &[200]), vec![200]);
        // The next request is also a reply: it starts a set of its own.
        let a = w.arrive(8);
        assert_eq!(each(&w, a, 300), Each::Marked);
        assert_eq!(w.marks(), 1);
        check(&w);
    }

    /// An inode named twice by one reply is one mark (a hard link listed
    /// twice).
    #[test]
    fn an_inode_named_twice_by_a_reply_is_one_mark() {
        let w = watch();
        let a = w.arrive(7);
        for ino in [100, 101, 100, 100] {
            assert_eq!(each(&w, a, ino), Each::Marked);
        }
        assert_eq!((w.marks(), w.held()), (2, 2));
        check(&w);
        assert_eq!(change(&w, &[100]), vec![100]);
        assert_eq!(w.marks(), 1);
        check(&w);
    }

    /// A single-inode reply replaces the task's whole set, and gives its
    /// room back.
    #[test]
    fn a_single_inode_reply_replaces_the_set() {
        let w = watch();
        let a = w.arrive(7);
        for ino in [100, 101, 102] {
            each(&w, a, ino);
        }
        let again = w.arrive(7);
        assert_eq!((w.marks(), w.held()), (0, 0));
        assert!(!reply(&w, again, 100));
        assert!(!reply(&w, again, 101));
        assert_eq!((w.marks(), w.held()), (1, 1));
        check(&w);
        assert!(change(&w, &[100]).is_empty());
        assert_eq!(change(&w, &[101]), vec![101]);
    }

    /// An entry whose inode changed while the page was being built gets
    /// its notice at once; the others stay marked.
    #[test]
    fn a_change_during_a_page_notices_that_entry_only() {
        let w = watch();
        let a = w.arrive(7);
        assert_eq!(each(&w, a, 100), Each::Marked);
        // 101 changes after the request arrived, before its entry is built.
        assert!(change(&w, &[101]).is_empty(), "no mark for 101 yet");
        assert_eq!(each(&w, a, 101), Each::Noticed);
        assert_eq!(each(&w, a, 102), Each::Marked);
        assert_eq!(w.marks(), 2, "the notice released 101's mark");
        check(&w);
        // And a change of an earlier entry, during the page, finds it.
        assert_eq!(change(&w, &[100]), vec![100]);
        assert_eq!(each(&w, a, 103), Each::Marked);
        assert_eq!(w.marks(), 2);
        check(&w);
    }

    /// A pid-0 page marks each inode on its own (superseding, one per
    /// inode, as a single reply does).
    #[test]
    fn a_pid_0_page_marks_each_inode_once() {
        let w = watch();
        for _ in 0..3 {
            let a = w.arrive(0);
            for ino in [100, 101, 102] {
                assert_eq!(each(&w, a, ino), Each::Marked);
            }
        }
        assert_eq!(w.marks(), 3, "one mark per inode");
        check(&w);
        assert_eq!(change(&w, &[101]), vec![101]);
        assert_eq!(w.marks(), 2);
        // Its "next request" clears nothing.
        w.arrive(0);
        assert_eq!(change(&w, &[100, 102]), vec![100, 102]);
        assert_eq!(w.marks(), 0);
        check(&w);
    }

    /// The bound counts inodes, not tasks: one task's page can fill the
    /// table, and a task with room for none more is told so.
    #[test]
    fn the_bound_counts_inodes() {
        let (w, _) = watch_of(5);
        let a = w.arrive(1);
        for ino in 100..103 {
            assert_eq!(each(&w, a, ino), Each::Marked);
        }
        let b = w.arrive(2);
        for ino in 200..202 {
            assert_eq!(each(&w, b, ino), Each::Marked);
        }
        assert_eq!(w.marks(), 5);
        // Full: another inode of a task that has a set, a new task.
        assert_eq!(each(&w, a, 103), Each::Full);
        assert_eq!(each(&w, b, 203), Each::Full);
        let c = w.arrive(3);
        assert_eq!(each(&w, c, 300), Each::Full);
        assert_eq!(w.marks(), 5, "nothing was added");
        assert_eq!(w.full.load(Ordering::Relaxed), 3);
        // An inode the set holds takes no room.
        assert_eq!(each(&w, a, 101), Each::Marked);
        check(&w);
        // The marks stay: a Full left the page's earlier entries covered.
        assert_eq!(change(&w, &[100]), vec![100]);
        // The notice made room for one inode; a task's next request makes
        // room for its whole set.
        assert_eq!(each(&w, c, 300), Each::Marked);
        assert_eq!(each(&w, c, 301), Each::Full);
        w.arrive(2);
        assert_eq!(w.marks(), 3);
        assert_eq!(each(&w, c, 301), Each::Marked);
        assert_eq!(w.marks(), 4);
        check(&w);
        // pid 0 has room of its own, and counts inodes too.
        for ino in 400..405 {
            let a = w.arrive(0);
            assert_eq!(each(&w, a, ino), Each::Marked);
        }
        let a = w.arrive(0);
        assert_eq!(each(&w, a, 405), Each::Full);
        // A re-read of a marked inode needs no room.
        let a = w.arrive(0);
        assert_eq!(each(&w, a, 404), Each::Marked);
        assert_eq!(w.marks(), 9);
        check(&w);
    }

    /// A reply of several inodes is told `Full`; a single-inode reply
    /// writes its notice at once, as before.
    #[test]
    fn a_full_table_answers_full_to_a_many_inode_reply_and_notices_a_single_one() {
        let (w, _) = watch_of(3);
        let a = w.arrive(1);
        for ino in 100..103 {
            assert_eq!(each(&w, a, ino), Each::Marked);
        }
        let b = w.arrive(2);
        assert_eq!(each(&w, b, 200), Each::Full);
        assert!(matches!(w.replying_each(b, 200), Replying::Full));
        assert_eq!(w.marks(), 3);
        // The single-inode path, with the same arrival: a notice, no mark.
        assert!(reply(&w, b, 200), "the notice goes before this reply");
        assert_eq!(w.marks(), 3);
        // That notice released the marks of its inode (here none); one of
        // an inode that has one frees it.
        assert!(
            reply(&w, w.arrive(3), 101),
            "full: noticed, and 101 released"
        );
        assert_eq!(w.marks(), 2);
        assert_eq!(each(&w, b, 200), Each::Marked, "room again");
        assert_eq!(w.full.load(Ordering::Relaxed), 4);
        check(&w);
    }

    /// An exited task's marks are swept as a set to make room for a page.
    #[test]
    fn a_page_makes_room_by_sweeping_the_sets_of_exited_tasks() {
        let (w, gone) = watch_of(4);
        let a = w.arrive(1);
        for ino in 100..104 {
            each(&w, a, ino);
        }
        let b = w.arrive(2);
        assert_eq!(each(&w, b, 200), Each::Full);
        gone.lock().unwrap().insert(1);
        std::thread::sleep(SWEEP_EVERY + Duration::from_millis(20));
        let c = w.arrive(3);
        assert_eq!(each(&w, c, 300), Each::Marked, "the set of 1 was swept");
        assert_eq!(w.marks(), 1);
        check(&w);
        // A change that finds one mark of a gone task drops that mark.
        let (w, gone) = watch_of(10);
        let a = w.arrive(1);
        for ino in 100..103 {
            each(&w, a, ino);
        }
        gone.lock().unwrap().insert(1);
        assert!(change(&w, &[100]).is_empty());
        assert_eq!(w.marks(), 2);
        check(&w);
    }

    /// An untracked request leaves no mark, for a page too.
    #[test]
    fn an_untracked_page_leaves_no_mark() {
        let w = watch();
        for ino in 1..4 {
            assert_eq!(each(&w, Arrival::untracked(), ino), Each::Marked);
        }
        assert_eq!(w.marks(), 0);
    }

    /// A release spares a newer set of the task.
    #[test]
    fn a_release_spares_a_newer_set_of_the_task() {
        let w = watch();
        let a = w.arrive(7);
        each(&w, a, 100);
        each(&w, a, 101);
        let (_, marks) = w.changed(&[100, 101]);
        // The task installed, sent its next request and got another page.
        let a = w.arrive(7);
        each(&w, a, 100);
        each(&w, a, 102);
        w.release(marks);
        assert_eq!(w.marks(), 2, "the new set stays");
        check(&w);
        assert_eq!(change(&w, &[100, 102]), vec![100, 102]);
        assert_eq!(w.held(), 0);
    }

    /// Both orders of a page's reply against changes of its inodes, from
    /// two threads: each inode's notice is written by one of them.
    #[test]
    fn a_page_and_changes_never_both_miss_each_other() {
        use std::sync::Barrier;
        let w = Arc::new(watch());
        let missed = Arc::new(AtomicUsize::new(0));
        for round in 0..1000u64 {
            let base = 20_000 + round * 4;
            let go = Arc::new(Barrier::new(2));
            let (w1, go1) = (w.clone(), go.clone());
            let reader = std::thread::spawn(move || {
                let a = w1.arrive(1);
                go1.wait();
                (0..4)
                    .map(|i| each(&w1, a, base + i) == Each::Noticed)
                    .collect::<Vec<_>>()
            });
            go.wait();
            let mut found = Vec::new();
            for i in (0..4).rev() {
                found.extend(change(&w, &[base + i]));
            }
            let wrote = reader.join().unwrap();
            for i in 0..4 {
                if !wrote[i as usize] && !found.contains(&(base + i)) {
                    missed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        assert_eq!(missed.load(Ordering::Relaxed), 0);
        check(&w);
    }

    /// Pages, single replies, requests, changes and exits from eight
    /// threads leave the two maps consistent, the bound held, and the table
    /// empty once every task has gone on.
    #[test]
    fn pages_keep_the_maps_consistent_under_threads() {
        let (w, gone) = watch_of(48);
        let w = Arc::new(w);
        let handles: Vec<_> = (0..8u32)
            .map(|t| {
                let (w, gone) = (w.clone(), gone.clone());
                std::thread::spawn(move || {
                    let mut x = 0x9e37_79b9_u64 + u64::from(t);
                    for step in 0..10_000u64 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        // Each thread owns its task ids (a task has one
                        // request outstanding).
                        let tid = ((x % 12) as u32 + 1) * 8 + t;
                        let ino = (x >> 8) % 24;
                        match (x >> 20) % 8 {
                            0..=1 => {
                                let a = w.arrive(tid);
                                for k in 0..=(x >> 32) % 6 {
                                    each(&w, a, (ino + k) % 24);
                                }
                            }
                            2 => {
                                let a = w.arrive(tid);
                                reply(&w, a, ino);
                            }
                            3 => {
                                w.arrive(tid);
                            }
                            4 => {
                                change(&w, &[ino, (ino + 1) % 24, (ino + 2) % 24]);
                            }
                            5 => {
                                let a = w.arrive(0);
                                each(&w, a, ino);
                            }
                            6 => {
                                let a = w.arrive(0);
                                reply(&w, a, ino);
                            }
                            _ => {
                                let mut g = gone.lock().unwrap();
                                if step % 2 == 0 {
                                    g.insert(tid);
                                } else {
                                    g.remove(&tid);
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        check(&w);
        gone.lock().unwrap().clear();
        for tid in 1..=(13 * 8 + 8) {
            w.arrive(tid);
        }
        for ino in 0..26 {
            change(&w, &[ino]);
        }
        assert_eq!(w.marks(), 0, "every task went on, every inode noticed");
        check(&w);
    }

    /// The real probe: this process's threads exist, an id nobody has
    /// does not (the largest pid the kernel allows is below 2^22).
    #[test]
    fn the_probe_tells_a_live_task_from_a_gone_one() {
        assert!(task_exists(std::process::id()));
        let mut child = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let pid = child.id();
        assert!(task_exists(pid));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(!task_exists(pid), "reaped");
        assert!(!task_exists(i32::MAX as u32 - 1));
    }

    /// What the bookkeeping costs, for the record: `cargo test --release -p
    /// constellation-frontend-fuse --lib -- --ignored --nocapture
    /// attrwatch::tests::cost`.
    #[test]
    #[ignore = "a measurement"]
    fn cost() {
        // The hot path: one task's request and reply, per thread, with a
        // change of another inode now and then.
        for threads in [1u32, 4, 8] {
            let w = Arc::new(AttrWatch::default());
            let n = 500_000u64;
            let t = Instant::now();
            let hs: Vec<_> = (0..threads)
                .map(|k| {
                    let w = w.clone();
                    std::thread::spawn(move || {
                        for i in 0..n {
                            let a = w.arrive(std::process::id() + k);
                            reply(&w, a, 100 + u64::from(k));
                            if i % 64 == 0 {
                                change(&w, &[7_000_000 + i]);
                            }
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            let ns = t.elapsed().as_nanos() as u64 / n;
            println!("hot path, {threads} threads: {ns} ns per request+reply");
        }
        // A full table of exited tasks, swept; one inode with many marks of
        // exited tasks, found by one change.
        // (Task ids above any pid_max: the real probe says they are gone.)
        let w = AttrWatch::default();
        for tid in 0..MARKS_MAX as u32 {
            let a = w.arrive(0x7000_0000 + tid);
            reply(&w, a, u64::from(tid));
        }
        let t = Instant::now();
        let freed = w.sweep();
        println!(
            "sweep of {MARKS_MAX} marks of unknown tasks: {} us (freed: {freed}, left {})",
            t.elapsed().as_micros(),
            w.marks()
        );
        let w = AttrWatch::default();
        for tid in 1..=10_000u32 {
            let a = w.arrive(0x7000_0000 + tid);
            reply(&w, a, 5);
        }
        let t = Instant::now();
        let found = change(&w, &[5]);
        println!(
            "one change finding 10000 marks of a gone-task kind: {} us (found {found:?}, left {})",
            t.elapsed().as_micros(),
            w.marks()
        );
        // Real short-lived processes: each replies once and exits; the
        // table is then swept.
        let w = AttrWatch::default();
        let t0 = Instant::now();
        let procs = 3000;
        for i in 0..procs {
            let mut c = std::process::Command::new("true").spawn().unwrap();
            let pid = c.id();
            let a = w.arrive(pid);
            reply(&w, a, 1 + (i % 50) as u64);
            c.wait().unwrap();
        }
        let spawn = t0.elapsed();
        println!(
            "{procs} processes spawned, marked and reaped: {} ms, {} marks held",
            spawn.as_millis(),
            w.marks()
        );
        let t = Instant::now();
        let freed = w.sweep();
        println!(
            "sweep after the churn: {} us (freed {freed}, left {})",
            t.elapsed().as_micros(),
            w.marks()
        );
    }
}
