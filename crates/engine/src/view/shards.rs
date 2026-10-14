//! Write sessions and per-inode ordering: [`WriteState`], [`WriteShards`],
//! [`InodeOps`].

use super::*;

/// In-flight write state for one inode: bytes live on disk in `staging`
/// (bounded RAM regardless of file size, plan 07), not in a `Vec` per
/// chunk. The staging file's dirty set tracks which chunk indices have
/// staged content, and `written` which of their bytes; everything else
/// (up to `file_len`) is the committed manifest's — `base` for the
/// flush (or whatever it rebases onto), the manifest current at the
/// read for a read ([`View::chunk_image`]).
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
    /// The sequential runs this session's writes are in, `[start, end)`,
    /// least recently extended first (at most `MAX_RUNS`): a write that
    /// starts inside one extends it, any other starts a new one. The
    /// chunks a run moves past are sealed for eager upload
    /// ([`super::View::note_write`]).
    pub(super) runs: Vec<(u64, u64)>,
    /// The file's mtime as this session leaves it: now, at each data
    /// change ([`WriteState::changed`]); the time an `utimes`/`futimens`
    /// set since ([`WriteState::times_set`]). `None`: no change yet. The
    /// flush commits it, and `getattr` shows it meanwhile.
    pub(super) mtime_ns: Option<i64>,
    /// When the session last changed the file's data: its ctime
    /// meanwhile, unless the row's (an attribute change since) is later.
    pub(super) ctime_ns: i64,
    /// The bytes this session's `write()` calls actually delivered
    /// (absolute, merged half-open ranges), as opposed to the rest of a
    /// dirty chunk, which is the base's. Two things depend on that split:
    ///
    /// - The flush replays just these ranges over whatever base it
    ///   composes or rebases onto (124cc95), so a chunk several nodes
    ///   patch disjointly converges on the union of their writes instead
    ///   of each writer's copy of the base clobbering the others' bytes.
    /// - Only these ranges of a dirty chunk are read from staging, by
    ///   reads, seals and the flush alike ([`View::chunk_image`]); the
    ///   rest comes from the base, so a chunk's first partial write
    ///   stages only its own bytes. (Until finding 09 it also copied the
    ///   whole committed chunk into staging — a seed only reads used,
    ///   since the flush already ignored it: a 4 MiB read and write per
    ///   chunk per session, ~40 MiB staged per `fsync` for the 128 KiB
    ///   an 8 KiB random writer `fsync`ing every 16 writes wrote.)
    ///
    /// So staging must hold the latest written byte at every offset in
    /// these ranges of a dirty chunk, and nothing else of a dirty chunk
    /// is read from it but its staged base (`staged_base`). A chunk
    /// whose staged bytes were released while ranges here survive —
    /// sealed (`release_chunk`: its staging slot goes to another chunk,
    /// whose bytes it then holds), punched whole by `fallocate`
    /// (`zeroed`) — gets them back before it is staged again: the sealed
    /// copy, or zeros (`do_write`, the truncate straddle).
    pub(super) written: Written,
    /// Dirty chunks whose staging also holds the bytes of a base chunk
    /// (`hash`, `len` bytes long) at every offset below `until` that
    /// `written` leaves to the base: put there by the read that had to
    /// fetch that chunk ([`View::stage_base`]). While the chunk's base is
    /// still that chunk — content addressed, so the bytes are the same —
    /// reads, seals and the flush take it from staging instead of
    /// fetching it again, which with a cache too small to keep it means
    /// from S3 (a GET of a whole chunk per small read of a dirty chunk).
    /// An entry goes whenever staging stops holding those bytes: the
    /// chunk is sealed, punched, re-staged from a copy, cut by a
    /// truncate, or a write into it fails.
    pub(super) staged_base: HashMap<u64, StagedBase>,
    /// Sealed chunks whose sealed copy holds a base chunk's bytes (below
    /// `until`) at every offset `written` leaves to the base: the base
    /// the seal composed it over. Staged again from that copy (a write
    /// into the chunk, a truncate cutting it), the chunk has that base
    /// staged (`staged_base`) and needs no fetch of it — under staging
    /// pressure a random writer's chunks are sealed and staged again
    /// over and over, and with a small cache each round fetched the
    /// base anew.
    pub(super) sealed_base: HashMap<u64, StagedBase>,
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

/// A set of absolute byte offsets kept as sorted, disjoint, non-adjacent
/// half-open ranges (`start -> end`): what a write session's writes
/// delivered ([`WriteState::written`]). Merged on insert, so its size is
/// the number of disjoint regions written, never the number of writes,
/// and a lookup is a `BTreeMap` range query — every read of a dirty
/// chunk asks it, and the flush once per dirty chunk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct Written {
    ranges: std::collections::BTreeMap<u64, u64>,
}

impl Written {
    /// Add `[start, end)`, merging it with every range it overlaps or
    /// touches.
    pub(super) fn insert(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let (mut start, mut end) = (start, end);
        if let Some((&s, &e)) = self.ranges.range(..=start).next_back() {
            if e >= start {
                start = s;
                end = end.max(e);
            }
        }
        // Disjoint and non-adjacent: a range starting past `end` cannot
        // reach back into one merged here, so one pass suffices.
        let merged: Vec<u64> = self.ranges.range(start..=end).map(|(s, _)| *s).collect();
        for s in merged {
            let e = self.ranges.remove(&s).expect("listed");
            end = end.max(e);
        }
        self.ranges.insert(start, end);
    }

    /// Drop every offset at or past `at` (a truncate).
    pub(super) fn clip(&mut self, at: u64) {
        self.ranges.split_off(&at);
        if let Some((_, e)) = self.ranges.iter_mut().next_back() {
            *e = (*e).min(at);
        }
    }

    /// The parts of the set inside `[start, end)`, in order.
    pub(super) fn overlapping(
        &self,
        start: u64,
        end: u64,
    ) -> impl Iterator<Item = (u64, u64)> + '_ {
        let before = self
            .ranges
            .range(..start)
            .next_back()
            .filter(|(_, e)| **e > start)
            .map(|(s, e)| (*s, *e));
        let inside = self
            .ranges
            .range(start..end.max(start))
            .map(|(s, e)| (*s, *e));
        before
            .into_iter()
            .chain(inside)
            .map(move |(s, e)| (s.max(start), e.min(end)))
            .filter(|(s, e)| s < e)
    }

    /// Whether the set holds every offset of `[start, end)`.
    pub(super) fn covers(&self, start: u64, end: u64) -> bool {
        if start >= end {
            return true;
        }
        // Merged: only one range can cover it.
        self.ranges
            .range(..=start)
            .next_back()
            .is_some_and(|(_, e)| *e >= end)
    }

    #[cfg(test)]
    pub(super) fn ranges(&self) -> Vec<(u64, u64)> {
        self.ranges.iter().map(|(s, e)| (*s, *e)).collect()
    }
}

/// See [`WriteState::staged_base`].
#[derive(Clone, Copy, Debug)]
pub(super) struct StagedBase {
    pub(super) hash: ChunkHash,
    pub(super) len: u64,
    pub(super) until: u64,
}

impl WriteState {
    /// How many bytes from `chunk_start` of base chunk `hash` (for chunk
    /// `idx`) are valid below `floor` and staged, if staging holds every
    /// valid one below `need_end` (the end of what the caller reads).
    /// The staged extent stops at the session's length when it was
    /// staged; a base another node extended since is valid further, past
    /// what a read of the session can ask for (it ends at the session's
    /// length) — requiring all of it re-fetched and re-staged the chunk
    /// on every read. The flush, which may compose the chunk at the
    /// extended length, fetches it.
    pub(super) fn staged_base_valid(
        &self,
        idx: u64,
        hash: &ChunkHash,
        chunk_start: u64,
        floor: u64,
        need_end: u64,
    ) -> Option<u64> {
        let staged = self.staged_base.get(&idx).filter(|s| s.hash == *hash)?;
        let valid = staged.len.min(floor.saturating_sub(chunk_start));
        let held = staged.until.saturating_sub(chunk_start);
        (valid <= held || need_end <= staged.until).then_some(valid.min(held))
    }
}

/// The committed content a write session's chunk images lay its writes
/// over ([`View::chunk_image`]): a manifest's chunk list and its
/// `file_len` (its content is valid only below it).
pub(super) struct ImageBase<'a> {
    pub(super) chunks: &'a SparseChunks,
    pub(super) file_len: u64,
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
    pub(super) detached: [Mutex<HashMap<Ino, Pending>>; WRITE_SHARDS],
}

/// What `getattr` overlays from a write session on the committed row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Pending {
    pub(super) len: u64,
    pub(super) mtime_ns: Option<i64>,
    pub(super) ctime_ns: i64,
}

impl Pending {
    /// `attr` as the session leaves it.
    pub(super) fn overlay(&self, attr: &mut FileAttr) {
        attr.size = self.len;
        if let Some(mtime) = self.mtime_ns {
            attr.mtime_ns = mtime;
        }
        attr.ctime_ns = attr.ctime_ns.max(self.ctime_ns);
    }
}

impl WriteState {
    pub(super) fn pending(&self) -> Pending {
        Pending {
            len: self.file_len,
            mtime_ns: self.mtime_ns,
            ctime_ns: self.ctime_ns,
        }
    }

    /// A data change (write, truncate, fallocate): mtime and ctime now,
    /// on the HLC every other inode time comes from (plan 30 §M12): above
    /// every stamp this node has issued or applied. The commit carries
    /// the ctime ([`Self::commit_ctime`]) and leaves the file the later of
    /// it and the row's (`constellation_meta::store::writes::commit_ctime`).
    ///
    /// Above the row's ctime of an attribute change made before it once
    /// that change's record is applied here. Not yet, on a node whose
    /// `fchmod` went to a sequencer whose clock is ahead: then the write
    /// can be stamped below it, and the commit leaves the `fchmod`'s
    /// ctime with the write's (earlier) mtime — the write's ctime late by
    /// at most that skew, never a ctime going back. (The sequencer
    /// observing this stamp at the commit covers the opposite skew, a
    /// writer ahead of it; nothing this node has can cover this one.)
    pub(super) fn changed(&mut self) {
        let now = constellation_meta::hlc::now_ns();
        self.mtime_ns = Some(now);
        self.ctime_ns = now;
    }

    /// What the commit carries as the writer's last data change: the
    /// session's ctime. A session that changed no data is never committed
    /// ([`Self::changed_nothing`]); `None` (the commit's time) only for
    /// one that sealed a chunk all the same — a write that failed after
    /// staging pressure sealed part of it.
    pub(super) fn commit_ctime(&self) -> Option<i64> {
        (self.ctime_ns > 0).then_some(self.ctime_ns)
    }

    /// Whether the session changed no data and holds nothing to publish:
    /// every data change stamps it ([`Self::changed`]), so only a refused
    /// first write or truncate (quota, staging space) leaves one, maybe
    /// with an mtime a `futimens` set since (already on the row: its
    /// `setattr` committed it). Its close publishes nothing — a close is
    /// no change (DESIGN §3 "File times"); committing it restamped both
    /// times with the commit's.
    pub(super) fn changed_nothing(&self) -> bool {
        self.ctime_ns == 0 && self.sealed.is_empty() && self.enrolled.is_empty()
    }

    /// An explicit mtime (`utimes`/`futimens`) while the session is open:
    /// the flush must commit it, not the last write's time (`cp -p` sets
    /// the source's times before its `close`). Not a ctime of the
    /// session's: the `setattr` that set it stamped the row's, which
    /// `getattr` and the commit both take, so a later stamp here would be
    /// one the commit drops — ctime moving back at the close.
    ///
    /// Folded under the inode's [`InodeOps`] lock, with the session in
    /// its shard: a set that found it taken out by a flush already past
    /// its detach lost to that flush's commit of the write's time.
    pub(super) fn times_set(&mut self, mtime_ns: i64) {
        self.mtime_ns = Some(mtime_ns);
    }
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
        self.pending(shard, ino).map(|p| p.len)
    }

    /// What `ino`'s session overlays on the committed row (size, times):
    /// in the map, or detached. Call with the inode's shard lock held.
    pub(super) fn pending(&self, shard: &HashMap<Ino, WriteState>, ino: Ino) -> Option<Pending> {
        shard.get(&ino).map(WriteState::pending).or_else(|| {
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
            .insert(ino, ws.pending());
        Some(ws)
    }

    /// Whether `ino` has a write session, attached or detached (a session
    /// is retired only once its commit has returned, or it is dropped
    /// unpublished).
    pub(super) fn has_session(&self, ino: Ino) -> bool {
        let shard = self.lock(ino);
        self.pending(&shard, ino).is_some()
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
/// session (`read`, `write`, `truncate`, `fallocate`, `lseek`, an mtime
/// set while one is open, and the flush that publishes it), held across
/// their slow parts — the chunk fetch, the forward, the drain — so the
/// per-inode order a single shard lock used to give survives
/// [`WriteShards`] releasing it. Only operations on the same inode
/// wait. Re-entrant on one thread (`fallocate` truncates and writes
/// through the public paths).
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

    /// [`Self::lock`] if it would not wait: `None` while another thread
    /// holds `ino` (this thread's own hold re-enters, as `lock` does).
    pub(super) fn try_lock(&self, ino: Ino) -> Option<InodeOpGuard<'_>> {
        let me = std::thread::current().id();
        let (m, _) = &self.shards[ino as usize % 64];
        let mut held = m.lock().unwrap_or_else(|e| e.into_inner());
        match held.get_mut(&ino) {
            None => {
                held.insert(ino, (me, 1));
            }
            Some((owner, depth)) if *owner == me => *depth += 1,
            Some(_) => return None,
        }
        Some(InodeOpGuard { ops: self, ino })
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

    fn written(ranges: &[(u64, u64)]) -> Written {
        let mut w = Written::default();
        for &(s, e) in ranges {
            w.insert(s, e);
        }
        w
    }

    #[test]
    fn covers_needs_the_whole_range() {
        assert!(written(&[(0, 13)]).covers(0, 13));
        assert!(written(&[(5, 13), (0, 6)]).covers(0, 13));
        assert!(written(&[(0, 100)]).covers(10, 20));
        assert!(!written(&[(0, 5), (6, 13)]).covers(0, 13));
        assert!(!written(&[(0, 12)]).covers(0, 13));
        assert!(!written(&[]).covers(0, 1));
        // Touching ranges merge.
        assert!(written(&[(0, 5), (5, 13)]).covers(0, 13));
        assert_eq!(written(&[(5, 13), (0, 5)]).ranges(), vec![(0, 13)]);
    }

    /// `Written` against a bitmap of written offsets: random inserts,
    /// clips, range queries and coverage checks.
    #[test]
    fn written_matches_a_bitmap() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        for _ in 0..2000 {
            let mut w = Written::default();
            let mut bits = [false; 96];
            for _ in 0..12 {
                let a = next(96);
                let b = (a + next(20)).min(96);
                if next(5) == 0 {
                    w.clip(a);
                    bits[a as usize..].fill(false);
                } else {
                    w.insert(a, b);
                    bits[a as usize..b as usize].fill(true);
                }
                let ranges = w.ranges();
                for pair in ranges.windows(2) {
                    assert!(pair[0].1 < pair[1].0, "disjoint, non-adjacent: {ranges:?}");
                }
                let (qa, qb) = {
                    let a = next(96);
                    (a, (a + next(40)).min(96))
                };
                let mut got = [false; 96];
                for (s, e) in w.overlapping(qa, qb) {
                    assert!(qa <= s && s < e && e <= qb);
                    got[s as usize..e as usize].fill(true);
                }
                for i in 0..96 {
                    let want = bits[i] && (qa as usize..qb as usize).contains(&i);
                    assert_eq!(got[i], want, "offset {i} of [{qa}, {qb}) in {ranges:?}");
                }
                assert_eq!(
                    w.covers(qa, qb),
                    bits[qa as usize..qb as usize].iter().all(|b| *b),
                    "[{qa}, {qb}) in {ranges:?}"
                );
            }
        }
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
