//! [`KernelEntries`]: the names this connection's kernel can hold a
//! *valid* dentry for, so that a `FUSE_NOTIFY_INVAL_ENTRY` is written only
//! when it can drop something.
//!
//! # Why
//!
//! An entry notification is the one write on `/dev/fuse` that is sure to
//! take a sleeping lock: `fuse_reverse_inval_entry` (Linux 7.3, `fs/fuse/
//! dir.c`) takes the parent directory's `i_rwsem` with
//! `inode_lock_nested` — not interruptible, not killable — *before* it
//! looks the name up, so it waits for every syscall holding that
//! directory, and such a syscall holds it from before its request is
//! queued to this daemon until every request it makes is answered
//! (`kernel_inval`'s module doc in the engine has the cycle a `kill -9`
//! then closes: the dead daemon's requests end only when its last
//! `/dev/fuse` descriptor closes, which needs the thread blocked in the
//! notification to exit first). The engine holds a notification back
//! while a request on the directory is in flight, but it cannot see a
//! syscall that holds the lock and whose request is still queued in the
//! kernel, nor the gaps between the requests of one syscall (`unlink` is a
//! `LOOKUP` then an `UNLINK`, `getdents` several `READDIR`s, all under the
//! one lock). Under a storm of other nodes' creates and unlinks in a
//! directory this node is busy in, the notification thread spent much of
//! its time parked on that lock — and the `fuse-inval-storm` scenario's
//! `kill -9` kept landing there.
//!
//! Most of those notifications could not have dropped anything. This
//! daemon never answers a lookup with a negative entry (a missing name is
//! `ENOENT`, which the kernel caches as an already expired dentry it
//! always looks up again), so the only dentries a notification can make a
//! difference to are positive ones the kernel got from one of *our*
//! replies — `LOOKUP`, `CREATE`, `MKDIR`, `MKNOD`, `SYMLINK`, `LINK` — and
//! keeps valid for the entry TTL that reply carried, or that a local
//! `RENAME` carried over to the new name (the kernel moves the dentry).
//! An expired dentry is looked up again before use, exactly as an
//! invalidated one. A rename that moves a name while another request that
//! may install it is still in flight (a revalidating `LOOKUP` of the old
//! name, which the kernel sends without the directory lock) carries the
//! longest TTL any reply may grant over to the new name, since that reply
//! will land on the moved dentry but be recorded under the old name. A name another node created, renamed or removed that
//! this node's kernel has not been handed within its TTL — every other
//! node's name, in the storm — needs no notification at all.
//!
//! # The rule
//!
//! A name may be cached while a request that can install its dentry is in
//! flight, and until the TTL of the last such reply, plus a slack of as
//! much again ([`slack`]), has passed. The slack is there because the
//! kernel starts the dentry's TTL later than the reply: the task that sent
//! the request stamps it (`fuse_change_entry_timeout` in `fuse_lookup`,
//! revalidation and create) only once it has woken up from the answer —
//! on both transports, and on io_uring only after the commit the daemon
//! handed over has been processed — and a task descheduled for a while in
//! between (a CPU-throttled pod, a loaded host) stamps it that much later.
//! A whole TTL of delay is not bounded either, but past it a stale dentry
//! lasts at most one more TTL, the bound the design already accepts for a
//! dropped notification; the cost of the slack is only a few more
//! notifications. The in-flight part keeps the
//! kernel's own ordering: a notification decided while a `LOOKUP` that
//! read the replica before the change is still unanswered is written,
//! and the kernel applies it after that answer (the lookup holds the
//! directory lock the notification needs). A request that only reaches
//! the adapter after the decision reads the replica after the change.
//!
//! Names are keyed by a 64-bit hash: a collision only makes a name look
//! cached, which costs one notification, never a missed one.
//!
//! A resumed session (plan 31 §6.11) inherits a kernel whose dentries the
//! previous server handed out: nothing is filtered for [`RESUMED`] after
//! the resume, longer than any entry TTL a view grants (1 s, or less for
//! a lone strict node).

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The least slack added to a reply's TTL, for tiny TTLs (jiffy rounding).
const MIN_SLACK: Duration = Duration::from_millis(100);

/// What is added to a reply's `ttl` (see the module doc): as much again.
pub(crate) fn slack(ttl: Duration) -> Duration {
    ttl.max(MIN_SLACK)
}

/// The longest entry TTL a view grants (the engine's `View::ttl`: 1 s);
/// a longer one any reply has carried counts instead
/// ([`KernelEntries::longest_ttl`]).
const MAX_TTL: Duration = Duration::from_secs(1);

/// How long a resumed session filters nothing (see the module doc).
pub(crate) const RESUMED: Duration = Duration::from_secs(5);

const SHARDS: usize = 16;

/// A shard is swept of expired names once it has grown to this many, and
/// then whenever it has doubled since the last sweep.
const SWEEP_AT: usize = 1024;

type Key = (u64, u64);

#[derive(Default)]
struct Slot {
    /// Requests in flight that can install this name's dentry.
    inflight: u32,
    /// When the last reply's dentry expires in the kernel.
    expires: Option<Instant>,
}

impl Slot {
    fn live(&self, now: Instant) -> bool {
        self.inflight > 0 || self.expires.is_some_and(|e| e > now)
    }
}

#[derive(Default)]
struct Shard {
    slots: HashMap<Key, Slot>,
    sweep_at: usize,
}

impl Shard {
    fn sweep_if_due(&mut self, now: Instant) {
        if self.slots.len() < self.sweep_at.max(SWEEP_AT) {
            return;
        }
        self.slots.retain(|_, s| s.live(now));
        self.sweep_at = self.slots.len() * 2;
    }
}

/// See the module doc. One per connection's adapter, shared with its
/// notification sink.
pub struct KernelEntries {
    shards: [Mutex<Shard>; SHARDS],
    hasher: RandomState,
    /// Nothing is filtered before this (a resumed session).
    unfiltered_until: Mutex<Option<Instant>>,
    /// The longest TTL a reply has carried, in nanoseconds.
    longest_ttl_ns: AtomicU64,
}

impl Default for KernelEntries {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::default()),
            hasher: RandomState::new(),
            unfiltered_until: Mutex::new(None),
            longest_ttl_ns: AtomicU64::new(0),
        }
    }
}

impl KernelEntries {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// This connection was served by another process (or another session)
    /// until now: filter nothing for [`RESUMED`].
    pub(crate) fn resumed(&self) {
        *self.unfiltered_until.lock().unwrap() = Some(Instant::now() + RESUMED);
    }

    fn key(&self, parent: u64, name: &[u8]) -> Key {
        (parent, self.hasher.hash_one(name))
    }

    fn shard(&self, key: &Key) -> &Mutex<Shard> {
        let i = (key.0 ^ key.1.rotate_left(17)) as usize % SHARDS;
        &self.shards[i]
    }

    /// A request that can install `name`'s dentry under `parent` has
    /// arrived; the name counts as cached until the guard is done with.
    pub(crate) fn begin(self: &Arc<Self>, parent: u64, name: &[u8]) -> EntryGuard {
        let key = self.key(parent, name);
        self.shard(&key)
            .lock()
            .unwrap()
            .slots
            .entry(key)
            .or_default()
            .inflight += 1;
        EntryGuard {
            entries: self.clone(),
            key,
            done: false,
        }
    }

    fn end(&self, key: Key, ttl: Option<Duration>) {
        let now = Instant::now();
        if let Some(ttl) = ttl {
            let ns = u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX);
            self.longest_ttl_ns.fetch_max(ns, Ordering::Relaxed);
        }
        let mut shard = self.shard(&key).lock().unwrap();
        if let Some(slot) = shard.slots.get_mut(&key) {
            slot.inflight = slot.inflight.saturating_sub(1);
            if let Some(ttl) = ttl.filter(|t| !t.is_zero()) {
                let expires = expiry_after(now, ttl);
                slot.expires = Some(slot.expires.map_or(expires, |e| e.max(expires)));
            }
        }
        shard.sweep_if_due(now);
    }

    /// The longest TTL a reply may carry: [`MAX_TTL`], or a longer one a
    /// reply has carried.
    fn longest_ttl(&self) -> Duration {
        MAX_TTL.max(Duration::from_nanos(
            self.longest_ttl_ns.load(Ordering::Relaxed),
        ))
    }

    /// Requests in flight for `key`.
    fn inflight(&self, key: &Key) -> u32 {
        self.shard(key)
            .lock()
            .unwrap()
            .slots
            .get(key)
            .map_or(0, |s| s.inflight)
    }

    fn expiry(&self, key: &Key) -> Option<Instant> {
        self.shard(key)
            .lock()
            .unwrap()
            .slots
            .get(key)
            .and_then(|s| s.expires)
    }

    fn extend(&self, key: Key, expires: Instant) {
        let mut shard = self.shard(&key).lock().unwrap();
        let slot = shard.slots.entry(key).or_default();
        slot.expires = Some(slot.expires.map_or(expires, |e| e.max(expires)));
    }

    /// Whether the kernel may hold a valid dentry for `name` in `parent`.
    pub(crate) fn may_be_cached(&self, parent: u64, name: &[u8]) -> bool {
        let now = Instant::now();
        if self
            .unfiltered_until
            .lock()
            .unwrap()
            .is_some_and(|until| now < until)
        {
            return true;
        }
        let key = self.key(parent, name);
        self.shard(&key)
            .lock()
            .unwrap()
            .slots
            .get(&key)
            .is_some_and(|s| s.live(now))
    }

    /// How many names are tracked (tests).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().slots.len())
            .sum()
    }
}

/// When a dentry the kernel got at `now` with `ttl` may expire there, with
/// the [`slack`].
fn expiry_after(now: Instant, ttl: Duration) -> Instant {
    now.checked_add(ttl.saturating_add(slack(ttl)))
        .unwrap_or(now + Duration::from_secs(365 * 24 * 3600))
}

/// One request that can install a dentry ([`KernelEntries::begin`]):
/// [`Self::replied`] with the entry TTL the reply carried, or dropped
/// (an error, a dropped responder) to install nothing.
pub(crate) struct EntryGuard {
    entries: Arc<KernelEntries>,
    key: Key,
    done: bool,
}

impl EntryGuard {
    /// The reply handed the kernel this name with `ttl`.
    pub(crate) fn replied(mut self, ttl: Duration) {
        self.done = true;
        self.entries.end(self.key, Some(ttl));
    }
}

impl Drop for EntryGuard {
    fn drop(&mut self) {
        if !self.done {
            self.entries.end(self.key, None);
        }
    }
}

/// A `RENAME`'s two names: the kernel moves the source's dentry to the
/// target (and with `RENAME_EXCHANGE` the target's to the source), keeping
/// its expiry, so a successful rename carries each moved name's expiry
/// over. Both count as cached while the rename is in flight.
pub(crate) struct RenameGuard {
    from: EntryGuard,
    to: EntryGuard,
}

impl RenameGuard {
    pub(crate) fn new(
        entries: &Arc<KernelEntries>,
        (parent, name): (u64, &[u8]),
        (new_parent, new_name): (u64, &[u8]),
    ) -> Self {
        Self {
            from: entries.begin(parent, name),
            to: entries.begin(new_parent, new_name),
        }
    }

    /// The rename succeeded (`exchange`: both names moved).
    pub(crate) fn renamed(self, exchange: bool) {
        let entries = &self.from.entries;
        let (from, to) = (self.from.key, self.to.key);
        // What the moved dentry may still be valid for: its recorded
        // expiry, or — while another request that can install the old
        // name is in flight (beyond this rename's own count), whose reply
        // will revalidate the moved dentry but be recorded under the old
        // name — the longest a reply may grant.
        let moved = |key: &Key| {
            let recorded = entries.expiry(key);
            if entries.inflight(key) > 1 {
                let longest = expiry_after(Instant::now(), entries.longest_ttl());
                Some(recorded.map_or(longest, |e| e.max(longest)))
            } else {
                recorded
            }
        };
        let (from_expiry, to_expiry) = (moved(&from), moved(&to));
        if let Some(e) = from_expiry {
            entries.extend(to, e);
        }
        if exchange {
            if let Some(e) = to_expiry {
                entries.extend(from, e);
            }
        }
        // Dropping the guards ends both in-flight counts.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_never_handed_out_is_not_cached() {
        let e = KernelEntries::new();
        assert!(!e.may_be_cached(1, b"x"));
    }

    #[test]
    fn a_name_is_cached_while_its_request_is_in_flight_and_for_its_ttl_after() {
        let e = KernelEntries::new();
        let g = e.begin(1, b"x");
        assert!(e.may_be_cached(1, b"x"), "in flight");
        assert!(!e.may_be_cached(1, b"y"));
        assert!(!e.may_be_cached(2, b"x"));
        let ttl = Duration::from_millis(150);
        let replied = Instant::now();
        g.replied(ttl);
        // The kernel may stamp the TTL as late as a whole TTL after the
        // reply (a descheduled caller): still cached up to then.
        std::thread::sleep(ttl + ttl / 2);
        if replied.elapsed() < ttl * 2 {
            assert!(e.may_be_cached(1, b"x"), "within its TTL and slack");
        }
        std::thread::sleep(ttl + Duration::from_millis(30));
        assert!(!e.may_be_cached(1, b"x"), "expired");
    }

    #[test]
    fn an_error_or_a_zero_ttl_installs_nothing() {
        let e = KernelEntries::new();
        drop(e.begin(1, b"x"));
        assert!(!e.may_be_cached(1, b"x"));
        e.begin(1, b"y").replied(Duration::ZERO);
        assert!(!e.may_be_cached(1, b"y"));
    }

    #[test]
    fn a_later_shorter_reply_does_not_shorten_an_earlier_longer_one() {
        let e = KernelEntries::new();
        e.begin(1, b"x").replied(Duration::from_secs(60));
        e.begin(1, b"x").replied(Duration::ZERO);
        assert!(e.may_be_cached(1, b"x"));
    }

    #[test]
    fn concurrent_requests_keep_a_name_in_flight_until_the_last_ends() {
        let e = KernelEntries::new();
        let a = e.begin(1, b"x");
        let b = e.begin(1, b"x");
        drop(a);
        assert!(e.may_be_cached(1, b"x"));
        drop(b);
        assert!(!e.may_be_cached(1, b"x"));
    }

    #[test]
    fn a_rename_carries_the_moved_dentrys_expiry_to_the_new_name() {
        let e = KernelEntries::new();
        e.begin(1, b"old").replied(Duration::from_secs(60));
        let r = RenameGuard::new(&e, (1, b"old"), (2, b"new"));
        assert!(e.may_be_cached(2, b"new"), "in flight");
        r.renamed(false);
        assert!(e.may_be_cached(2, b"new"));
        // A rename of a name the kernel did not hold moves nothing.
        RenameGuard::new(&e, (1, b"cold"), (2, b"target")).renamed(false);
        assert!(!e.may_be_cached(2, b"target"));
        // An exchange moves both ways.
        e.begin(3, b"b").replied(Duration::from_secs(60));
        RenameGuard::new(&e, (3, b"a"), (3, b"b")).renamed(true);
        assert!(e.may_be_cached(3, b"a"));
        // A failed rename moves nothing.
        drop(RenameGuard::new(&e, (1, b"old"), (4, b"failed")));
        assert!(!e.may_be_cached(4, b"failed"));
    }

    /// Should-fix of the fuse-inval-hang review: a revalidating `LOOKUP`
    /// of the old name, in flight while the rename completes, revalidates
    /// the moved dentry when it is answered — but under the old name. The
    /// new name counts as cached for as long as any reply may grant.
    #[test]
    fn a_rename_during_a_lookup_of_the_old_name_keeps_the_new_name_cached() {
        let e = KernelEntries::new();
        let lookup = e.begin(1, b"old");
        RenameGuard::new(&e, (1, b"old"), (2, b"new")).renamed(false);
        lookup.replied(MAX_TTL);
        assert!(e.may_be_cached(2, b"new"));
        let key = e.key(2, b"new");
        let expires = e.expiry(&key).expect("an expiry carried over");
        assert!(
            expires >= Instant::now() + MAX_TTL + slack(MAX_TTL) - Duration::from_millis(500),
            "only {:?} left",
            expires - Instant::now()
        );
        // No lookup in flight: nothing beyond the recorded expiry moves.
        RenameGuard::new(&e, (3, b"cold"), (3, b"target")).renamed(false);
        assert!(!e.may_be_cached(3, b"target"));
        // A longer TTL some reply carried counts as the longest.
        e.begin(5, b"long").replied(Duration::from_secs(30));
        let lookup = e.begin(5, b"old");
        RenameGuard::new(&e, (5, b"old"), (5, b"new")).renamed(false);
        drop(lookup);
        let left = e.expiry(&e.key(5, b"new")).unwrap() - Instant::now();
        assert!(left > Duration::from_secs(50), "{left:?}");
    }

    #[test]
    fn a_resumed_connection_filters_nothing_at_first() {
        let e = KernelEntries::new();
        e.resumed();
        assert!(e.may_be_cached(1, b"handed out by the previous server"));
    }

    #[test]
    fn expired_names_are_swept() {
        let e = KernelEntries::new();
        for i in 0..(SHARDS * SWEEP_AT * 2) as u64 {
            drop(e.begin(i, b"x"));
        }
        assert!(e.len() < SHARDS * SWEEP_AT, "{} names kept", e.len());
        let held = e.begin(7, b"held");
        for i in 0..(SHARDS * SWEEP_AT * 2) as u64 {
            drop(e.begin(i, b"y"));
        }
        assert!(
            e.may_be_cached(7, b"held"),
            "a name in flight survives a sweep"
        );
        drop(held);
    }
}
