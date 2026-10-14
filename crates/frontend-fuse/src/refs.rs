//! [`KernelRefs`]: how many lookups of each node id this connection's
//! kernel holds, so that [`Vfs::forget`](constellation_vfs::Vfs::forget)
//! — "the platform no longer references the inode at all" — is called
//! only when the kernel has let go of every one (plan 40 §4.4, R2).
//!
//! # Why the adapter counts
//!
//! The kernel keeps a lookup count per inode (`fuse_inode::nlookup`): one
//! for each reply that handed it the node id — `LOOKUP` (a revalidation's
//! included), `CREATE`, `MKNOD`, `MKDIR`, `SYMLINK`, `LINK`, and each
//! `READDIRPLUS` entry with a node id — and a `FORGET(ino, n)` gives `n`
//! of them back. Most `FORGET`s are final: the kernel sends the whole
//! count when it evicts the inode (`fuse_evict_inode`). Some are partial,
//! sent while the kernel still holds the inode through another dentry, an
//! open file or an `O_PATH` descriptor:
//!
//! - a dentry revalidation whose `LOOKUP` answers another node id than the
//!   dentry's forgets that answer's single lookup
//!   (`fuse_dentry_revalidate`, `fs/fuse/dir.c`) — the inode it names may
//!   well be held under another name (another node renamed it over the
//!   revalidated one);
//! - an entry reply the kernel cannot instantiate (an allocation failure
//!   in `fuse_iget`: `fuse_lookup_name`, `create_new_entry`,
//!   `fuse_create_open`) forgets its one lookup;
//! - a `READDIRPLUS` entry the kernel fails to link is force-forgotten
//!   (`fuse_force_forget`, `fs/fuse/readdir.c`, `nlookup = 1`), whatever
//!   else holds the inode (plan 40 §3 K4; not served yet).
//!
//! The view reaps an orphan it kept for the kernel's reference at its
//! `forget` (`View::forget_unlinked`, 9447477), so a partial `FORGET`
//! forwarded as is reaps an inode the kernel still references: its
//! descriptor then answers `ESTALE`. Until plan 40 R2 the adapter
//! forwarded every `FORGET`, arguing that the revalidation one names an
//! inode a name just led to, never an unlinked one. That holds when the
//! reply is written, not when the `FORGET` is read: the kernel queues it
//! after the answer, and another worker may read it after a later
//! `LOOKUP` + `UNLINK` of the same inode — the last name — has been
//! applied (`fs/fuse/dev.c` even lets requests pass queued forgets).
//! Plan 31 §6.11 had decided against a lookup table because inode numbers
//! need none across a handover; the partial forgets need one.
//!
//! # The rule
//!
//! Every reply that hands the kernel a node id counts it ([`took`]) —
//! **before** the reply is written, since a `FORGET` for it can follow
//! the reply at once and be read by another worker — and only once the
//! reply is a success. A `FORGET` subtracts ([`forgot`]):
//!
//! - [`Forgot::Last`]: the count reached zero, the view is told.
//! - [`Forgot::Partial`]: the kernel still holds the inode; nothing.
//! - [`Forgot::Underflow`]: the kernel gave back more than was counted, so
//!   some reply that handed out the node id was not counted and the kernel
//!   may hold more lookups than this table knows. The entry goes, but the
//!   view is not told (logged at warn, counted).
//! - [`Forgot::Unknown`]: a node id never counted here. The root's
//!   reference comes from the mount, not from a reply, and the kernel does
//!   not forget it while mounted; a `FORGET` of the root is forwarded (a
//!   no-op for the view). Any other unknown node id is a count lost — a
//!   reply path that does not count, which is the very bug this table
//!   exists for — so the view is not told (logged at warn by the adapter,
//!   counted).
//!
//! Every doubt keeps the inode: an orphan kept for a reference the kernel
//! no longer holds is a bounded leak (the node's reaper takes it once the
//! session ends), an orphan reaped while the kernel holds it is `ESTALE`
//! for a descriptor. Likewise a reply the kernel never processes (a
//! connection abort, or one of the error paths where the kernel drops a
//! reply without forgetting it) over-counts: the inode is kept until the
//! session ends, never reaped early.
//!
//! # Cost
//!
//! One shard lock and a hash-map update per entry reply and per
//! `FORGET`, with [`SHARDS`] shards; about 32 bytes per inode the kernel
//! holds (its own inode and dentry cost well over 1 KiB).
//!
//! # Handover
//!
//! The counts cross a session handover with the connection
//! ([`KernelRefs::export`], `FuseHandoff::refs`), taken once every worker
//! has stopped and imported before the next server reads: the `FORGET`s
//! the kernel queued during the gap are read by the next server and
//! subtract from the imported counts. In the `daemon --upgrade` handoff
//! (JSON) each inode the kernel holds costs about 20 bytes: some 20 MB for
//! a million cached inodes.
//!
//! [`took`]: KernelRefs::took
//! [`forgot`]: KernelRefs::forgot

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex};

/// Shards of the table: entry replies come from every worker at once.
const SHARDS: usize = 64;

/// A shard is shrunk once it holds less than a quarter of its capacity
/// and its capacity is at least this (the kernel shed a large cache).
const SHRINK_AT: usize = 4096;

/// The counts that cross a handover: `(node id, lookups)`.
pub type RefsHandoff = Vec<(u64, u64)>;

/// What a `FORGET` did ([`KernelRefs::forgot`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Forgot {
    /// The kernel let go of its last lookup.
    Last,
    /// The kernel still holds some.
    Partial,
    /// The kernel gave back more than was counted: some lookups were
    /// missed, it may still hold the inode (module doc).
    Underflow,
    /// Never counted here (module doc).
    Unknown,
}

/// The splitmix64 finalizer: shards take the low bits of a node id, so a
/// shard's map needs a hash in which every bit depends on every other.
#[derive(Default)]
struct Mix(u64);

impl Hasher for Mix {
    fn finish(&self) -> u64 {
        let mut z = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = (self.0 << 8) | u64::from(*b);
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

/// One shard: its counts, and the outcomes of the `FORGET`s it saw (kept
/// under the shard's lock rather than in shared atomics, which every
/// worker's `FORGET` would contend on).
#[derive(Default)]
struct Shard {
    counts: HashMap<u64, u64, BuildHasherDefault<Mix>>,
    last: u64,
    partial: u64,
    underflow: u64,
    unknown: u64,
}

/// A shard on cache lines of its own: neighbouring shards' locks would
/// otherwise share one, and every worker would contend on it.
#[derive(Default)]
#[repr(align(128))]
struct Padded(Mutex<Shard>);

/// See the module doc. One per connection's adapter.
pub struct KernelRefs {
    shards: [Padded; SHARDS],
}

impl Default for KernelRefs {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Padded::default()),
        }
    }
}

impl std::fmt::Debug for KernelRefs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KernelRefs")
            .field("inodes", &self.len())
            .finish_non_exhaustive()
    }
}

impl KernelRefs {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn shard(&self, ino: u64) -> &Mutex<Shard> {
        &self.shards[(ino % SHARDS as u64) as usize].0
    }

    /// A reply about to hand the kernel `ino` (one lookup).
    pub(crate) fn took(&self, ino: u64) {
        *self
            .shard(ino)
            .lock()
            .unwrap()
            .counts
            .entry(ino)
            .or_insert(0) += 1;
    }

    /// The kernel gave back `n` lookups of `ino` (`FORGET`).
    pub(crate) fn forgot(&self, ino: u64, n: u64) -> Forgot {
        let mut guard = self.shard(ino).lock().unwrap();
        let shard = &mut *guard;
        let outcome = {
            let counts = &mut shard.counts;
            match counts.get_mut(&ino) {
                None => Forgot::Unknown,
                Some(count) if *count > n => {
                    *count -= n;
                    Forgot::Partial
                }
                Some(count) => {
                    let outcome = if *count < n {
                        tracing::warn!(
                            ino,
                            counted = *count,
                            forgotten = n,
                            "the kernel forgot more lookups of an inode than were counted \
                             (a reply that handed it out was not counted); the inode is kept"
                        );
                        Forgot::Underflow
                    } else {
                        Forgot::Last
                    };
                    counts.remove(&ino);
                    let len = counts.len();
                    if counts.capacity() >= SHRINK_AT && len < counts.capacity() / 4 {
                        counts.shrink_to(len * 2);
                    }
                    outcome
                }
            }
        };
        *match outcome {
            Forgot::Last => &mut shard.last,
            Forgot::Partial => &mut shard.partial,
            Forgot::Underflow => &mut shard.underflow,
            Forgot::Unknown => &mut shard.unknown,
        } += 1;
        outcome
    }

    fn sum(&self, of: impl Fn(&Shard) -> u64) -> u64 {
        self.shards.iter().map(|s| of(&s.0.lock().unwrap())).sum()
    }

    /// The counts, for the next server of the connection. Only meaningful
    /// once nothing replies or forgets any more (a stopped session).
    pub fn export(&self) -> RefsHandoff {
        let mut refs = Vec::with_capacity(self.len());
        for shard in &self.shards {
            let shard = shard.0.lock().unwrap();
            refs.extend(shard.counts.iter().map(|(i, n)| (*i, *n)));
        }
        refs
    }

    /// Take over a previous server's counts (before serving).
    pub(crate) fn import(&self, refs: &RefsHandoff) {
        for &(ino, n) in refs {
            if n > 0 {
                *self
                    .shard(ino)
                    .lock()
                    .unwrap()
                    .counts
                    .entry(ino)
                    .or_insert(0) += n;
            }
        }
    }

    /// Node ids the kernel holds at least one lookup of.
    pub fn len(&self) -> usize {
        self.sum(|s| s.counts.len() as u64) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The lookups counted for `ino` (0: none).
    pub fn count(&self, ino: u64) -> u64 {
        self.shard(ino)
            .lock()
            .unwrap()
            .counts
            .get(&ino)
            .copied()
            .unwrap_or(0)
    }

    /// `FORGET`s that let go of an inode's last lookup.
    pub fn forgets_final(&self) -> u64 {
        self.sum(|s| s.last)
    }

    /// `FORGET`s that left the inode referenced (not forwarded).
    pub fn forgets_partial(&self) -> u64 {
        self.sum(|s| s.partial)
    }

    /// `FORGET`s of node ids never counted (only the root's forwarded).
    pub fn forgets_unknown(&self) -> u64 {
        self.sum(|s| s.unknown)
    }

    /// `FORGET`s of more lookups than were counted (not forwarded).
    pub fn forgets_underflow(&self) -> u64 {
        self.sum(|s| s.underflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forget_is_final_only_for_the_last_lookup() {
        let refs = KernelRefs::default();
        refs.took(7);
        refs.took(7);
        refs.took(7);
        assert_eq!(refs.count(7), 3);
        assert_eq!(refs.forgot(7, 1), Forgot::Partial);
        assert_eq!(refs.count(7), 2);
        assert_eq!(refs.forgot(7, 2), Forgot::Last);
        assert_eq!(refs.count(7), 0);
        assert!(refs.is_empty());
        // Gone: a later FORGET is of an inode never counted.
        assert_eq!(refs.forgot(7, 1), Forgot::Unknown);
        assert_eq!(
            (
                refs.forgets_final(),
                refs.forgets_partial(),
                refs.forgets_unknown()
            ),
            (1, 1, 1)
        );
    }

    #[test]
    fn inodes_are_counted_apart() {
        let refs = KernelRefs::default();
        for ino in [1, 65, 129, 2] {
            refs.took(ino);
        }
        refs.took(65);
        // 1, 65 and 129 share a shard.
        assert_eq!(refs.forgot(1, 1), Forgot::Last);
        assert_eq!(refs.forgot(65, 1), Forgot::Partial);
        assert_eq!(refs.count(129), 1);
        assert_eq!(refs.count(2), 1);
        assert_eq!(refs.len(), 3);
    }

    #[test]
    fn the_roots_implicit_reference_is_unknown() {
        // The kernel's reference to the root comes from the mount, not
        // from a reply: a FORGET of it is never counted, and is forwarded.
        let refs = KernelRefs::default();
        assert_eq!(refs.forgot(1, 1), Forgot::Unknown);
        assert_eq!(refs.forgets_unknown(), 1);
    }

    #[test]
    fn an_underflow_is_never_final() {
        // Lookups were missed: the kernel may hold more than was counted,
        // so the inode is not let go of — the entry goes, the view is not
        // told.
        let refs = KernelRefs::default();
        refs.took(9);
        assert_eq!(refs.forgot(9, 5), Forgot::Underflow);
        assert_eq!(refs.count(9), 0);
        assert!(refs.is_empty());
        assert_eq!((refs.forgets_underflow(), refs.forgets_final()), (1, 0));
        refs.took(9);
        assert_eq!(refs.count(9), 1, "a new lookup counts from zero");
        assert_eq!(refs.forgot(9, 1), Forgot::Last);
    }

    #[test]
    fn counts_round_trip_a_handover_and_add_to_what_is_there() {
        let old = KernelRefs::default();
        for (ino, n) in [(3u64, 2u64), (1 << 40, 1), (u64::MAX, 4)] {
            for _ in 0..n {
                old.took(ino);
            }
        }
        let mut sent = old.export();
        sent.sort();
        assert_eq!(sent, vec![(3, 2), (1 << 40, 1), (u64::MAX, 4)]);
        let json = serde_json::to_string(&sent).unwrap();
        let got: RefsHandoff = serde_json::from_str(&json).unwrap();
        let new = KernelRefs::default();
        new.import(&got);
        assert_eq!(new.count(3), 2);
        assert_eq!(new.count(u64::MAX), 4);
        // A FORGET the kernel queued during the gap subtracts from the
        // imported count.
        assert_eq!(new.forgot(3, 1), Forgot::Partial);
        assert_eq!(new.forgot(1 << 40, 1), Forgot::Last);
        new.import(&vec![(3, 1), (8, 0)]);
        assert_eq!(new.count(3), 2);
        assert_eq!(new.count(8), 0, "a zero count is no reference");
    }

    #[test]
    fn concurrent_lookups_and_forgets_balance() {
        let refs = Arc::new(KernelRefs::default());
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let refs = refs.clone();
                std::thread::spawn(move || {
                    for round in 0..2_000u64 {
                        // Every thread takes and gives back the same
                        // inodes, interleaved with the others.
                        let ino = (round * 31 + t) % 97;
                        refs.took(ino);
                        refs.took(ino);
                        assert_ne!(refs.forgot(ino, 1), Forgot::Unknown);
                        let _ = refs.forgot(ino, 1);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(refs.is_empty(), "{} inodes still counted", refs.len());
        assert_eq!(refs.forgets_unknown(), 0);
        assert_eq!(refs.forgets_final() + refs.forgets_partial(), 8 * 2_000 * 2);
    }

    #[test]
    fn a_shard_shrinks_once_the_kernel_shed_its_cache() {
        let refs = KernelRefs::default();
        let inos: Vec<u64> = (0..20_000u64).map(|i| i * SHARDS as u64).collect();
        for &ino in &inos {
            refs.took(ino);
        }
        let grown = refs.shards[0].0.lock().unwrap().counts.capacity();
        for &ino in &inos[..19_900] {
            assert_eq!(refs.forgot(ino, 1), Forgot::Last);
        }
        let shrunk = refs.shards[0].0.lock().unwrap().counts.capacity();
        assert!(shrunk < grown / 4, "{grown} -> {shrunk}");
        assert_eq!(refs.len(), 100);
    }
}
