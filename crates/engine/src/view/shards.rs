//! Write sessions and per-inode ordering: [`WriteState`], [`WriteShards`],
//! [`InodeOps`].

use super::*;

/// In-flight write state for one inode: bytes live on disk in `staging`
/// (bounded RAM regardless of file size, plan 07), not in a `Vec` per
/// chunk. `dirty` tracks which chunk indices actually have staged
/// content; the rest of the file (up to `file_len`) is served from the
/// committed manifest in `base`.
pub(super) struct WriteState {
    pub(super) staging: Staging,
    pub(super) file_len: u64,
    pub(super) base: Option<Manifest>,
    pub(super) sealed: HashMap<u64, ChunkHash>,
    /// The sealed indices whose seal enrolled a pending-upload claim
    /// (`seal_crossed_chunks` skips the claim for content already known
    /// durable). Unsealing withdraws exactly that claim and no other:
    /// see [`View::unseal`].
    pub(super) enrolled: HashSet<u64>,
    pub(super) holes: crate::staging::DirtyRuns,
    /// Chunks this session discarded whole (punched or zeroed): their
    /// base content is gone even after a later write re-dirties them —
    /// unlike `holes`, a write does not clear this, so the composition
    /// starts such a chunk from zeros, not from the base.
    pub(super) zeroed: crate::staging::DirtyRuns,
    pub(super) seal_buffer: Vec<u8>,
    pub(super) high_water: u64,
    /// Absolute half-open byte ranges this open handle's `write()`
    /// calls actually delivered — as opposed to the padding bytes
    /// `do_write` seeds from the committed manifest so a partial
    /// write's untouched neighborhood reads back correctly within the
    /// handle. Kept separate because that seed is only as fresh as the
    /// manifest at the moment of the call: replaying just these ranges
    /// over whatever base a flush ultimately composes or rebases onto
    /// is what lets a chunk shared by several disjoint concurrent
    /// writers converge instead of each writer's seed silently
    /// clobbering the others' bytes with its own stale copy.
    pub(super) written: Vec<(u64, u64)>,
    /// The base's content is valid only below this offset: the lowest
    /// length the file was truncated to — by this session, or by a
    /// committed `setattr(size)` the base manifest predates (its
    /// `file_len` then exceeds the inode's size). Every read of base
    /// content for this session (a partial write's seed, a read of an
    /// untouched chunk, the flush's composition) zeroes what lies at or
    /// past it, so a later extension — by a write past a gap, a
    /// truncate-up, a `fallocate` — reads zeros there and never the
    /// bytes the truncate cut. `None`: nothing was truncated.
    pub(super) floor: Option<u64>,
}

impl WriteState {
    /// Where the content of a base manifest of length `base_len` stops
    /// being valid for this session: its own `file_len` (a manifest's
    /// content is valid only below it — a truncate lowers it, see
    /// `constellation_meta::replay::clip_manifest`), or this session's
    /// truncation, whichever is lower.
    pub(super) fn base_floor(&self, base_len: u64) -> u64 {
        self.floor.map_or(base_len, |f| f.min(base_len))
    }

    /// Zero what in `data` (content of a base of length `base_len`, the
    /// chunk starting at `chunk_start`) lies at or past
    /// [`WriteState::base_floor`].
    pub(super) fn clip_base(&self, data: &mut [u8], chunk_start: u64, base_len: u64) {
        clip_at(Some(self.base_floor(base_len)), data, chunk_start);
    }
}

/// Zero what in `data` (content starting at absolute offset `start`) lies
/// at or past `floor`.
pub(super) fn clip_at(floor: Option<u64>, data: &mut [u8], start: u64) {
    if let Some(floor) = floor {
        let keep = floor.saturating_sub(start).min(data.len() as u64) as usize;
        data[keep..].fill(0);
    }
}

pub(super) const WRITE_SHARDS: usize = 256;

/// Whether the half-open byte ranges `written` cover `[start, end)`
/// entirely.
pub(super) fn covers(written: &[(u64, u64)], start: u64, end: u64) -> bool {
    let mut ranges: Vec<(u64, u64)> = written
        .iter()
        .map(|&(s, e)| (s.max(start), e.min(end)))
        .filter(|(s, e)| s < e)
        .collect();
    ranges.sort_unstable();
    let mut reached = start;
    for (s, e) in ranges {
        if s > reached {
            return false;
        }
        reached = reached.max(e);
    }
    reached >= end
}

/// The open write sessions, sharded so unrelated files do not contend on
/// one process-wide mutex. A shard lock is only ever held for short,
/// local work: never across an S3 request, a forward, or a drain (EC2
/// finding 1: a close stuck on a black-holed S3 held its shard, and with
/// it the `getattr`/`lookup` of every inode sharing the shard — an
/// unrelated `ls -la` on that node hung for the whole outage). Ordering
/// between operations on the *same* inode is [`InodeOps`]' job.
///
/// `detached` carries the pending size of an inode whose session is out
/// of its map while an operation works on it without the shard lock (a
/// flush publishing it, a read overlaying it): `getattr` and `lookup`
/// read it under the shard lock exactly as they read a session's size.
pub(super) struct WriteShards {
    pub(super) maps: [Mutex<HashMap<Ino, WriteState>>; WRITE_SHARDS],
    pub(super) detached: [Mutex<HashMap<Ino, u64>>; WRITE_SHARDS],
}

impl WriteShards {
    pub(super) fn new() -> Self {
        Self {
            maps: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            detached: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn lock(&self, ino: Ino) -> std::sync::MutexGuard<'_, HashMap<Ino, WriteState>> {
        let m = &self.maps[ino as usize % WRITE_SHARDS];
        // Recover from poison instead of propagating it. If a fuser worker
        // panics while holding a shard guard, the guarded map is still
        // structurally valid — it maps inodes to their write sessions, and
        // a half-finished op does not break that invariant. Propagating the
        // poison (the old `unwrap()` / `panic!`) turned one worker's panic
        // into a cascade: every later op on any inode hashing to this shard
        // would panic too, killing workers one by one until the whole mount
        // wedged kernel-side (uninterruptible). Taking the inner guard, as
        // the rest of the codebase does (see `constellation_vfs::watch`), keeps a
        // poisoned shard from wedging the mount.
        match m.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                constellation_vfs::watch::stage("write shard lock");
                let guard = m.lock().unwrap_or_else(|e| e.into_inner());
                constellation_vfs::watch::stage("running");
                guard
            }
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
        }
    }

    /// The pending size of `ino`'s session: in the map, or detached. Call
    /// with the inode's shard lock held (`shard`).
    pub(super) fn pending_len(&self, shard: &HashMap<Ino, WriteState>, ino: Ino) -> Option<u64> {
        shard.get(&ino).map(|ws| ws.file_len).or_else(|| {
            self.detached[ino as usize % WRITE_SHARDS]
                .lock()
                .unwrap()
                .get(&ino)
                .copied()
        })
    }

    /// Take `ino`'s session out of its map (under the shard lock, so a
    /// `getattr` sees either the session or its detached size, never
    /// neither). The caller holds `ino`'s [`InodeOps`] lock and must
    /// [`Self::reattach`] (or [`Self::retire`]) it.
    pub(super) fn detach(&self, ino: Ino) -> Option<WriteState> {
        let mut shard = self.lock(ino);
        let ws = shard.remove(&ino)?;
        self.detached[ino as usize % WRITE_SHARDS]
            .lock()
            .unwrap()
            .insert(ino, ws.file_len);
        Some(ws)
    }

    /// Every inode with a write session, attached or detached
    /// (`View::sync_view`'s flush list).
    pub(super) fn pending_inos(&self) -> Vec<Ino> {
        let mut inos = Vec::new();
        for shard in 0..WRITE_SHARDS {
            inos.extend(self.lock_shard(shard).keys().copied());
            inos.extend(
                self.detached[shard]
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .keys()
                    .copied(),
            );
        }
        inos.sort_unstable();
        inos.dedup();
        inos
    }

    fn lock_shard(&self, shard: usize) -> std::sync::MutexGuard<'_, HashMap<Ino, WriteState>> {
        self.maps[shard].lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Put a detached session back.
    pub(super) fn reattach(&self, ino: Ino, ws: WriteState) {
        let mut shard = self.lock(ino);
        self.detached[ino as usize % WRITE_SHARDS]
            .lock()
            .unwrap()
            .remove(&ino);
        shard.insert(ino, ws);
    }

    /// A detached session is gone for good: published (its committed row
    /// now carries the size), or dropped by a failed flush.
    pub(super) fn retire(&self, ino: Ino) {
        let _shard = self.lock(ino);
        self.detached[ino as usize % WRITE_SHARDS]
            .lock()
            .unwrap()
            .remove(&ino);
    }
}

/// A flush that did not publish: the errno, and the session to put back
/// (`None`: it is gone — the failure happened before anything could be
/// retried from it, as it always was).
pub(super) struct FlushFail {
    pub(super) errno: Code,
    pub(super) ws: Option<Box<WriteState>>,
}

/// Per-inode ordering of the operations that read or change a write
/// session (`read`, `write`, `truncate`, `fallocate`, `lseek`, and the
/// flush that publishes it), held across their slow parts — the chunk
/// fetch, the forward, the drain — so the per-inode order a single
/// shard lock used to give survives [`WriteShards`] releasing it. Only
/// operations on the same inode wait. Re-entrant on one thread
/// (`fallocate` truncates and writes through the public paths).
pub(super) struct InodeOps {
    shards: [InodeOpShard; 64],
}

/// Held inodes (owner thread, depth) and the condvar their waiters sleep on.
pub(super) type InodeOpShard = (
    Mutex<HashMap<Ino, (std::thread::ThreadId, u32)>>,
    std::sync::Condvar,
);

pub(super) struct InodeOpGuard<'a> {
    ops: &'a InodeOps,
    ino: Ino,
}

impl InodeOps {
    pub(super) fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| {
                (Mutex::new(HashMap::new()), std::sync::Condvar::new())
            }),
        }
    }

    pub(super) fn lock(&self, ino: Ino) -> InodeOpGuard<'_> {
        let me = std::thread::current().id();
        let (m, cv) = &self.shards[ino as usize % 64];
        // Recover from poison rather than propagating it, for the same
        // reason as `WriteShards::lock`: the guarded map (inode -> owning
        // thread + re-entrancy depth) stays structurally valid across a
        // panic, and letting the poison through would panic every later op
        // on any inode hashing to this shard, wedging the mount worker by
        // worker. Matches `constellation_vfs::watch`'s `unwrap_or_else(|e| e.into_inner())`.
        let mut held = m.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            match held.get_mut(&ino) {
                None => {
                    held.insert(ino, (me, 1));
                    break;
                }
                Some((owner, depth)) if *owner == me => {
                    *depth += 1;
                    break;
                }
                Some(_) => {
                    constellation_vfs::watch::stage("inode operation lock");
                    held = cv.wait(held).unwrap_or_else(|e| e.into_inner());
                    constellation_vfs::watch::stage("running");
                }
            }
        }
        InodeOpGuard { ops: self, ino }
    }
}

impl Drop for InodeOpGuard<'_> {
    fn drop(&mut self) {
        let (m, cv) = &self.ops.shards[self.ino as usize % 64];
        // Recover from poison here too: a poisoned held-map must still be
        // updated on guard drop, or the depth bookkeeping leaks and waiters
        // on this shard never wake.
        let mut held = m.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, depth)) = held.get_mut(&self.ino) {
            *depth -= 1;
            if *depth == 0 {
                held.remove(&self.ino);
                drop(held);
                cv.notify_all();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covers_needs_the_whole_range() {
        assert!(covers(&[(0, 13)], 0, 13));
        assert!(covers(&[(5, 13), (0, 6)], 0, 13));
        assert!(covers(&[(0, 100)], 10, 20));
        assert!(!covers(&[(0, 5), (6, 13)], 0, 13));
        assert!(!covers(&[(0, 12)], 0, 13));
        assert!(!covers(&[], 0, 1));
    }

    #[test]
    fn same_inode_serializes_while_unrelated_inode_remains_available() {
        let writes = WriteShards::new();
        let _same_inode_guard = writes.lock(1);

        assert!(matches!(
            writes.maps[1].try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert!(writes.maps[2].try_lock().is_ok());
    }

    /// EC2 finding 1: the inode lock orders operations on one inode only,
    /// re-entrantly on one thread; another inode — even one sharing the
    /// write shard — is never held up by it.
    #[test]
    fn inode_ops_block_only_the_same_inode_and_reenter() {
        let ops = std::sync::Arc::new(InodeOps::new());
        let outer = ops.lock(7);
        let inner = ops.lock(7);
        drop(inner);
        // Same shard of `InodeOps` (and of `WriteShards`): free.
        let other = ops.lock(7 + 64 * WRITE_SHARDS as u64);
        drop(other);
        let (tx, rx) = std::sync::mpsc::channel();
        let t = {
            let ops = ops.clone();
            std::thread::spawn(move || {
                let _g = ops.lock(7);
                tx.send(()).unwrap();
            })
        };
        assert!(rx
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err());
        drop(outer);
        rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        t.join().unwrap();
    }
}
