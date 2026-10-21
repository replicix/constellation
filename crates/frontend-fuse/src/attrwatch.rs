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
//!   request (or a short life has passed, [`LIFE`]) the reply may still be
//!   on its way.
//!
//! Both are tracked without a lock on the common path:
//!
//! - A namespace operation that changed inodes records them in a ring
//!   ([`Changes`]) and counts them. An attribute request takes the count
//!   when it arrives ([`Arrival`]); when it replies for inode `i` and the
//!   count has moved, it looks `i` up in the ring and, if a change of `i`
//!   was recorded since, writes the notice itself, before its reply.
//! - A reply for inode `i` to task `t` is a mark `t -> i` until `t`'s next
//!   attribute or namespace request arrives ([`AttrWatch::arrive`]). A
//!   namespace operation finds the marks of the inodes it changed, writes
//!   a notice for each, and then discharges them ([`AttrWatch::release`]):
//!   the bump rejects every reply sent before it. Not before the notice is
//!   written: a second change of the inode in between must still find the
//!   mark and write its own notice before its reply.
//!
//! Each side stores its record, issues a full fence and then reads the
//! other's (store buffering), so at least one of them sees the other. A
//! reply that did not read the old state gets a needless notice, never a
//! missing one. Replies to requests of pid 0 (not visible in the mount's
//! pid namespace) cannot be cleared by a next request; their marks live
//! [`LIFE_UNKNOWN`].
//!
//! What is not covered: a reply installed more than [`LIFE`] after it was
//! generated (the mark has expired by then), so race 2b stays open for
//! that one reply. The time counts from [`AttrWatch::replying`], so it
//! includes the daemon's own delay before the kernel has the reply (over
//! a ring, the ring thread commits it on its next pass, after the other
//! requests it reaped with it were handled) as well as the task's between
//! its wake-up and the install. `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY=on`
//! notices every operation.

use crate::refs::Mix;
use constellation_vfs::Ino;
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a reply to a known task counts as possibly uninstalled when
/// the task sends nothing that clears it, from the moment it is generated.
/// A woken task installs its reply before it returns to user space; a
/// second is a reply that waited that long for the daemon to hand it over
/// or for its task to be scheduled (the module doc).
pub(crate) const LIFE: Duration = Duration::from_millis(1000);

/// The same for a reply to a request that carries no pid.
pub(crate) const LIFE_UNKNOWN: Duration = Duration::from_millis(50);

/// Changes remembered for the replies in flight. More than this many
/// namespace operations during one request counts as "changed".
const RING: usize = 256;

const SHARDS: usize = 16;

/// A shard that holds more marks than this drops the expired ones.
const PRUNE_AT: usize = 64;

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

type Marks = HashMap<u64, Mark, BuildHasherDefault<Mix>>;

#[derive(Debug, Clone, Copy)]
struct Mark {
    ino: Ino,
    expires: Instant,
    /// Unique per mark: [`AttrWatch::release`] removes the marks it found,
    /// not a newer one of the same task.
    id: u64,
}

/// Marks a notice is being written for, `(key, id)`: discharged by
/// [`AttrWatch::release`] once it is written.
#[must_use = "release the marks once the notice is written"]
#[derive(Debug, Default)]
pub(crate) struct Found(Vec<(u64, u64)>);

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
pub(crate) struct AttrWatch {
    changes: Changes,
    /// Marks by task id.
    marks: Vec<Mutex<Marks>>,
    /// Keys for the marks of requests without a pid (never cleared).
    anonymous: AtomicU64,
    /// [`Mark::id`]s.
    ids: AtomicU64,
    /// Marks held, expired ones included until they are dropped: a change
    /// finds none to discharge without taking a shard's lock when this is
    /// 0 (a single task's reply is installed before its next request, so
    /// its own changes find none).
    live: AtomicUsize,
}

impl Default for AttrWatch {
    fn default() -> Self {
        Self {
            changes: Changes::default(),
            marks: (0..SHARDS).map(|_| Mutex::default()).collect(),
            anonymous: AtomicU64::new(0),
            ids: AtomicU64::new(0),
            live: AtomicUsize::new(0),
        }
    }
}

impl AttrWatch {
    fn shard(&self, tid: u32) -> &Mutex<Marks> {
        &self.marks[tid as usize % SHARDS]
    }

    /// An attribute-carrying or namespace request of task `tid` arrives:
    /// whatever reply it was waiting for is installed. Returns what the
    /// request must remember until it replies.
    pub(crate) fn arrive(&self, tid: u32) -> Arrival {
        let cleared = tid != 0
            && self
                .shard(tid)
                .lock()
                .unwrap()
                .remove(&u64::from(tid))
                .is_some();
        if cleared {
            self.live.fetch_sub(1, Ordering::Relaxed);
        }
        Arrival {
            track: Some(Tracked {
                tid,
                seq: self.changes.seq.load(Ordering::Acquire),
            }),
        }
    }

    /// The request replies with the attributes of `ino`. `Some` when the
    /// reply must be preceded by a notice for `ino` (a change of it was
    /// recorded since the request arrived): the marks of `ino`, to
    /// [`Self::release`] once the notice is written.
    pub(crate) fn replying(&self, a: Arrival, ino: Ino) -> Option<Found> {
        let t = a.track?;
        let (key, life) = if t.tid == 0 {
            (
                (1 << 63) | self.anonymous.fetch_add(1, Ordering::Relaxed),
                LIFE_UNKNOWN,
            )
        } else {
            (u64::from(t.tid), LIFE)
        };
        {
            let mut shard = self.shard(key as u32).lock().unwrap();
            if shard.len() >= PRUNE_AT {
                let now = Instant::now();
                let before = shard.len();
                shard.retain(|_, m| m.expires > now);
                self.live.fetch_sub(before - shard.len(), Ordering::Relaxed);
            }
            let mark = Mark {
                ino,
                expires: Instant::now() + life,
                id: self.ids.fetch_add(1, Ordering::Relaxed),
            };
            if shard.insert(key, mark).is_none() {
                self.live.fetch_add(1, Ordering::Relaxed);
            }
        }
        fence(Ordering::SeqCst);
        if self.changes.seq.load(Ordering::Acquire) == t.seq {
            return None;
        }
        if self.changes.touched_since(t.seq, ino) {
            return Some(self.find(&[ino]).1);
        }
        None
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

    /// The live marks of `inos` (dropping expired ones): the inodes that
    /// have one, and the marks. They stay until [`Self::release`], so a
    /// concurrent change of the same inode still finds them.
    fn find(&self, inos: &[Ino]) -> (Vec<Ino>, Found) {
        let mut found = Vec::new();
        let mut marks = Found::default();
        if self.live.load(Ordering::Relaxed) == 0 {
            return (found, marks);
        }
        let now = Instant::now();
        for shard in &self.marks {
            let mut shard = shard.lock().unwrap();
            if shard.is_empty() {
                continue;
            }
            let before = shard.len();
            shard.retain(|&key, m| {
                if m.expires <= now {
                    return false;
                }
                if inos.contains(&m.ino) {
                    if !found.contains(&m.ino) {
                        found.push(m.ino);
                    }
                    marks.0.push((key, m.id));
                }
                true
            });
            self.live.fetch_sub(before - shard.len(), Ordering::Relaxed);
        }
        (found, marks)
    }

    /// The notice for the marks of `found` is written: discharge them (a
    /// mark the task's next request cleared, or a newer reply of the task
    /// replaced, is left alone).
    pub(crate) fn release(&self, found: Found) {
        for (key, id) in found.0 {
            let mut shard = self.shard(key as u32).lock().unwrap();
            if shard.get(&key).is_some_and(|m| m.id == id) {
                shard.remove(&key);
                self.live.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Marks held (tests, `node.status` has no use for it).
    #[cfg(test)]
    fn marks(&self) -> usize {
        self.marks.iter().map(|s| s.lock().unwrap().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn a_reply_not_yet_followed_by_a_request_is_a_mark_a_change_finds() {
        let w = AttrWatch::default();
        let a = w.arrive(7);
        assert!(!reply(&w, a, 100));
        assert_eq!(w.marks(), 1);
        assert_eq!(change(&w, &[100, 5]), vec![100]);
        // discharged: the next change finds nothing
        assert_eq!(change(&w, &[100]), Vec::<Ino>::new());
        assert_eq!(w.marks(), 0);
    }

    #[test]
    fn the_tasks_next_request_clears_its_mark() {
        let w = AttrWatch::default();
        let a = w.arrive(7);
        reply(&w, a, 100);
        w.arrive(7);
        assert_eq!(w.marks(), 0);
        assert_eq!(change(&w, &[100]), Vec::<Ino>::new());
    }

    #[test]
    fn another_tasks_request_clears_nothing_of_this_one() {
        let w = AttrWatch::default();
        let a = w.arrive(7);
        reply(&w, a, 100);
        w.arrive(8);
        assert_eq!(change(&w, &[100]), vec![100]);
    }

    #[test]
    fn a_change_during_a_request_makes_its_reply_write_the_notice() {
        let w = AttrWatch::default();
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
        let w = AttrWatch::default();
        change(&w, &[100]);
        let a = w.arrive(7);
        assert!(!reply(&w, a, 100));
    }

    #[test]
    fn a_ring_overflow_counts_as_a_change() {
        let w = AttrWatch::default();
        let a = w.arrive(7);
        for i in 0..(RING as u64 + 8) {
            change(&w, &[1000 + i]);
        }
        assert!(reply(&w, a, 100));
    }

    #[test]
    fn the_live_count_follows_the_marks() {
        let w = AttrWatch::default();
        let live = |w: &AttrWatch| w.live.load(Ordering::Relaxed);
        for tid in 1..=5 {
            let a = w.arrive(tid);
            reply(&w, a, 100 + u64::from(tid));
        }
        assert_eq!((live(&w), w.marks()), (5, 5));
        // A second reply of a task replaces its mark.
        let a = w.arrive(1);
        reply(&w, a, 200);
        assert_eq!((live(&w), w.marks()), (5, 5));
        w.arrive(2);
        assert_eq!((live(&w), w.marks()), (4, 4));
        assert_eq!(change(&w, &[103, 200]), vec![200, 103]);
        assert_eq!((live(&w), w.marks()), (2, 2));
        for s in &w.marks {
            for m in s.lock().unwrap().values_mut() {
                m.expires = Instant::now() - Duration::from_millis(1);
            }
        }
        assert!(change(&w, &[999]).is_empty());
        assert_eq!((live(&w), w.marks()), (0, 0));
        // Nothing live: a change takes no shard lock and finds nothing.
        assert!(change(&w, &[1]).is_empty());
    }

    #[test]
    fn an_untracked_request_leaves_no_mark() {
        let w = AttrWatch::default();
        assert!(!reply(&w, Arrival::untracked(), 100));
        assert_eq!(w.marks(), 0);
    }

    #[test]
    fn a_mark_expires() {
        let w = AttrWatch::default();
        let a = w.arrive(7);
        reply(&w, a, 100);
        // Age the mark by rewriting its expiry.
        for s in &w.marks {
            for m in s.lock().unwrap().values_mut() {
                m.expires = Instant::now() - Duration::from_millis(1);
            }
        }
        assert!(change(&w, &[100]).is_empty());
    }

    /// A change that found a mark has not written its notice yet: a
    /// second change of the inode still finds the mark, and writes its own
    /// notice before its reply (the first one's may come later).
    #[test]
    fn a_mark_stays_until_the_notice_that_found_it_is_written() {
        let w = AttrWatch::default();
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
    }

    /// The same for a reply that writes the notice itself: until it has,
    /// the other replies' marks of the inode stay for a change to find.
    #[test]
    fn a_late_replys_notice_discharges_marks_only_once_written() {
        let w = AttrWatch::default();
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
    }

    /// A release leaves a newer mark of the same task alone.
    #[test]
    fn a_release_spares_a_newer_mark_of_the_task() {
        let w = AttrWatch::default();
        let a = w.arrive(7);
        reply(&w, a, 100);
        let (_, marks) = w.changed(&[100]);
        // The task installed, sent its next request and got another reply.
        let a = w.arrive(7);
        reply(&w, a, 100);
        w.release(marks);
        assert_eq!(w.marks(), 1);
        assert_eq!(change(&w, &[100]), vec![100]);
        assert_eq!(w.live.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_reply_to_an_unknown_task_cannot_be_cleared_by_a_request_of_pid_0() {
        let w = AttrWatch::default();
        let a = w.arrive(0);
        reply(&w, a, 100);
        w.arrive(0);
        assert_eq!(change(&w, &[100]), vec![100]);
    }

    /// Both orders of a reply against a change, from two threads: the
    /// notice is always written by one of them (pins the protocol; the
    /// fences matter on weak-memory hosts, which this does not exercise).
    #[test]
    fn a_reply_and_a_change_never_both_miss_each_other() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::{Arc, Barrier};
        let w = Arc::new(AttrWatch::default());
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
}
