//! Per-subtree byte caps (plan 37 §5's `quota.set{subtree, bytes}`).
//!
//! A pool filesystem holds many Kubernetes volumes, one directory each
//! (`/volumes/<pv>`), and each volume's size is a cap on *its* subtree —
//! the filesystem-wide cap (`MetaStore::set_quota`) cannot express that.
//!
//! **Where the cap lives.** On the directory itself, as the xattr
//! [`SUBTREE_QUOTA_XATTR`], written through the ordinary journaled
//! `SetXattr`/`RemoveXattr` mutations (`View::mutate_op`, so it is
//! sequenced, forwarded and replicated exactly like any other xattr). That
//! buys three properties without a new log record: every node replays the
//! same cap; the cap travels with its directory through a rename (a pool
//! volume's trip to `/.trash`) and is copied by a clone or a snapshot like
//! the rest of the volume's record; and an old binary reading the journal
//! sees a plain xattr. The name has no `user.`/`trusted.` namespace, so the
//! xattr policy (`XattrPolicy::check_name`) refuses it to every FUSE caller
//! and to `browse.xattr` — root inside a workload pod cannot raise its own
//! cap — and `listxattr` leaves it out ([`is_internal_xattr`]); only
//! `quota.set` writes it.
//!
//! **What it binds.** A view mounted *at* a capped directory (the CSI
//! shape: one view per volume, rooted at `/volumes/<pv>`) checks growth
//! against the cap and the bytes under its own root, and reports the cap as
//! its `statfs` size. Writes arriving through a view mounted at an
//! *ancestor* of the capped directory (an operator's whole-filesystem
//! mount) are not charged against it: finding the nearest capped ancestor
//! of every written inode would cost a parent walk per write, and nothing
//! plan 37 mounts needs it. Like the filesystem-wide cap this is local,
//! best-effort admission, not a cluster-wide reservation (plan 37 calls the
//! quota soft).
//!
//! **What it compares against** ([`SubtreeUsage`]). The filesystem-wide
//! cap reads a maintained O(1) counter; a subtree has none, and its usage
//! is a walk of every entry under it (`MetaStore::recursive_size`, about
//! 0.45 µs per entry warm). Admission therefore uses the last walk *plus
//! the growth this view has admitted since*, per inode (the highest length
//! admitted over the inode's size when first seen), so N files written
//! inside one walk's lifetime are all charged — the bound on overshoot is
//! the writes in flight between a walk's start and its result, plus growth
//! through *other* views or nodes since the walk, not throughput × TTL.
//! The walk stays off the common write path: it runs once on the view's
//! first capped write, then only when a write would cross the cap and the
//! last walk is older than `statfs_ttl` (or the delta lost precision, see
//! [`GROWN_LIMIT`]) — which is also how deletions and truncations are
//! credited back: not at once, but by the re-walk the next write near the
//! cap triggers. A view far from its cap never walks again.

use super::*;

/// The xattr a directory's subtree cap is stored under (module docs).
pub const SUBTREE_QUOTA_XATTR: &str = "constellation.quota";

/// Engine-internal xattrs: no `user.`/`trusted.` namespace, so no frontend
/// caller can name them; hidden from `listxattr`.
pub(crate) fn is_internal_xattr(name: &str) -> bool {
    name.starts_with("constellation.")
}

impl View {
    /// Set (`Some`) or clear (`None`) the cap on directory `ino`'s subtree.
    /// `ino` is a real inode of this view's filesystem (the control plane
    /// resolves the path), and must be a directory other than the root,
    /// whose cap is the filesystem-wide one.
    pub(crate) fn set_subtree_quota(&self, ino: Ino, max_bytes: Option<u64>) -> Result<(), Code> {
        if ino == constellation_fs_core::types::ROOT_INO {
            return Err(Code::Invalid);
        }
        match self.meta.getattr(ino).map_err(|e| e.code())? {
            Some(attr) if attr.kind == InodeKind::Dir => {}
            Some(_) => return Err(Code::NotDir),
            None => return Err(Code::NotFound),
        }
        let op = match max_bytes {
            Some(cap) => constellation_meta::MutateOp::SetXattr {
                ino,
                name: SUBTREE_QUOTA_XATTR.to_string(),
                value: constellation_meta::store::quota_record(Some(cap)),
                mode: 0,
            },
            None => {
                if self.subtree_quota(ino)?.is_none() {
                    return Ok(());
                }
                constellation_meta::MutateOp::RemoveXattr {
                    ino,
                    name: SUBTREE_QUOTA_XATTR.to_string(),
                }
            }
        };
        self.mutate_op(ino, op)?;
        self.nudge_sync();
        Self::invalidate_quota_cache(&self.subtree_quota_cache);
        Ok(())
    }

    /// Shared handle so the engine can invalidate after a live `quota.set`.
    pub fn subtree_quota_cache_handle(&self) -> QuotaCache {
        self.subtree_quota_cache.clone()
    }

    /// The cap stored on directory `ino`, if any.
    pub(crate) fn subtree_quota(&self, ino: Ino) -> Result<Option<u64>, Code> {
        let Some(value) = self
            .meta
            .get_xattr(ino, SUBTREE_QUOTA_XATTR)
            .map_err(|e| e.code())?
        else {
            return Ok(None);
        };
        constellation_meta::store::parse_quota_record(&value).map_err(|e| e.code())
    }

    /// The cap on this view's own root, cached like the filesystem-wide
    /// one. `None` for a whole-filesystem view (the root's cap is the
    /// filesystem-wide cap) and for snapshot views (nothing grows there).
    pub(crate) fn cached_subtree_quota(&self) -> Option<u64> {
        let root = self.real_ino(constellation_fs_core::types::ROOT_INO);
        if root == constellation_fs_core::types::ROOT_INO || Self::is_synthetic(root) {
            return None;
        }
        {
            let guard = self.subtree_quota_cache.lock().unwrap();
            if let Some((fetched_at, cap)) = *guard {
                if fetched_at.elapsed() < QUOTA_CACHE_TTL {
                    return cap;
                }
            }
        }
        let cap = self.subtree_quota(root).ok().flatten();
        *self.subtree_quota_cache.lock().unwrap() = Some((Instant::now(), cap));
        cap
    }
}

/// Inodes the running delta tracks individually between walks. Past it the
/// map is dropped (its sum is kept): a later write to a dropped inode is
/// charged against its size at that moment, which can count bytes twice —
/// conservative — and marks the delta imprecise, so the next write that
/// would cross the cap re-walks at once instead of waiting out the TTL.
pub(crate) const GROWN_LIMIT: usize = 4096;

/// A view's subtree usage for cap admission (module docs).
#[derive(Debug, Default)]
pub(crate) struct SubtreeUsage {
    /// When the last walk ran and the bytes it found.
    walked: Option<(Instant, u64)>,
    /// Since the walk: inode → the highest length admitted for it (seeded
    /// with its committed size when first seen).
    grown: HashMap<Ino, u64>,
    /// Bytes admitted since the walk: the sum of growth over `grown`
    /// (including inodes already dropped from it).
    admitted: u64,
    /// `grown` was dropped since the walk ([`GROWN_LIMIT`]).
    imprecise: bool,
    /// Walks taken, for tests and the record of the cost.
    walks: u64,
}

impl SubtreeUsage {
    fn rewalk(&mut self, bytes: u64) {
        *self = SubtreeUsage {
            walked: Some((Instant::now(), bytes)),
            walks: self.walks + 1,
            ..SubtreeUsage::default()
        };
    }

    fn used(&self) -> u64 {
        self.walked
            .map_or(0, |(_, b)| b)
            .saturating_add(self.admitted)
    }

    /// The growth `new_len` adds over what is already charged for `ino`.
    fn extra(&self, ino: Ino, committed: u64, new_len: u64) -> u64 {
        let charged = self.grown.get(&ino).copied().unwrap_or(committed);
        new_len.saturating_sub(charged)
    }

    fn admit(&mut self, ino: Ino, committed: u64, new_len: u64, extra: u64) {
        if !self.grown.contains_key(&ino) && self.grown.len() >= GROWN_LIMIT {
            self.grown.clear();
            self.imprecise = true;
        }
        let entry = self.grown.entry(ino).or_insert(committed);
        *entry = (*entry).max(new_len);
        self.admitted = self.admitted.saturating_add(extra);
    }

    /// Whether a refusal may re-walk first: the walk is older than `ttl`,
    /// or the delta is no longer exact.
    fn may_rewalk(&self, ttl: Duration) -> bool {
        self.imprecise
            || self
                .walked
                .is_none_or(|(at, _)| ttl.is_zero() || at.elapsed() >= ttl)
    }
}

impl View {
    /// Admit growing `ino` (now `committed` bytes) to `new_len` under the
    /// view root's subtree cap `cap` (module docs). Holds the view's usage
    /// lock across a walk, so concurrent writes through this view wait for
    /// it rather than slip past the cap.
    pub(crate) fn subtree_admit(
        &self,
        ino: Ino,
        committed: u64,
        new_len: u64,
        cap: u64,
    ) -> Result<(), Code> {
        let mut usage = self.subtree_usage.lock().unwrap();
        if usage.walked.is_none() {
            usage.rewalk(self.subtree_walk());
        }
        let mut extra = usage.extra(ino, committed, new_len);
        if usage.used().saturating_add(extra) > cap {
            if !usage.may_rewalk(self.statfs_ttl) {
                return Err(Code::NoSpace);
            }
            usage.rewalk(self.subtree_walk());
            extra = usage.extra(ino, committed, new_len);
            if usage.used().saturating_add(extra) > cap {
                return Err(Code::NoSpace);
            }
        }
        usage.admit(ino, committed, new_len, extra);
        Ok(())
    }

    /// The bytes under the view's root, walked.
    fn subtree_walk(&self) -> u64 {
        let root = self.real_ino(constellation_fs_core::types::ROOT_INO);
        self.meta.recursive_size(root).map_or(0, |(bytes, _)| bytes)
    }

    #[cfg(test)]
    pub(crate) fn subtree_walks(&self) -> u64 {
        self.subtree_usage.lock().unwrap().walks
    }
}

/// [`statfs_blocks`], further bounded by the view's own subtree cap: free
/// space is whichever headroom runs out first.
pub(crate) fn statfs_blocks_capped(
    view_used: u64,
    fs_used: u64,
    fs_cap: Option<u64>,
    subtree_cap: Option<u64>,
    block: u64,
) -> (u64, u64) {
    let (total, free) = statfs_blocks(view_used, fs_used, fs_cap, block);
    match subtree_cap {
        None => (total, free),
        Some(cap) => {
            let free = free.min(cap.saturating_sub(view_used) / block);
            (view_used.div_ceil(block).saturating_add(free), free)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::quota_tests::test_fs;
    use super::*;
    use constellation_fs_core::types::ROOT_INO;

    /// `/volumes/pv` with a file of `len` bytes in it; returns the
    /// directory and the file.
    fn volume(meta: &Meta, len: u64) -> (Ino, Ino) {
        let volumes = meta.mkdir(ROOT_INO, "volumes", 0o755, 0, 0).unwrap();
        let pv = meta.mkdir(volumes.ino, "pv", 0o755, 0, 0).unwrap();
        let f = meta.create(pv.ino, "f", 0o644, 0, 0).unwrap();
        meta.setattr(f.ino, None, None, None, Some(len), None, None)
            .unwrap();
        (pv.ino, f.ino)
    }

    #[test]
    fn set_get_and_clear_a_subtree_cap() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (pv, f) = volume(&meta, 10);
        let (fs, _dir) = test_fs(meta.clone());
        assert_eq!(fs.subtree_quota(pv).unwrap(), None);
        fs.set_subtree_quota(pv, Some(100)).unwrap();
        assert_eq!(fs.subtree_quota(pv).unwrap(), Some(100));
        fs.set_subtree_quota(pv, Some(300)).unwrap();
        assert_eq!(fs.subtree_quota(pv).unwrap(), Some(300));
        fs.set_subtree_quota(pv, None).unwrap();
        assert_eq!(fs.subtree_quota(pv).unwrap(), None);
        // Clearing an absent cap is a no-op, not an ENODATA.
        fs.set_subtree_quota(pv, None).unwrap();
        // The root's cap is the filesystem-wide one; a file has no subtree.
        assert_eq!(
            fs.set_subtree_quota(ROOT_INO, Some(1)).unwrap_err(),
            Code::Invalid
        );
        assert_eq!(fs.set_subtree_quota(f, Some(1)).unwrap_err(), Code::NotDir);
        assert_eq!(
            fs.set_subtree_quota(9_999_999, Some(1)).unwrap_err(),
            Code::NotFound
        );
    }

    /// The cap is an ordinary journaled xattr: a peer replaying the journal
    /// gets it, and it is hidden from (and unnameable by) frontends.
    #[test]
    fn the_cap_replicates_and_is_internal() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (pv, _) = volume(&meta, 0);
        let (fs, _dir) = test_fs(meta.clone());
        fs.set_subtree_quota(pv, Some(4096)).unwrap();
        let records: Vec<_> = meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        let peer = Meta::open_in_memory().unwrap();
        peer.apply_records(&records).unwrap();
        let value = peer.get_xattr(pv, SUBTREE_QUOTA_XATTR).unwrap().unwrap();
        assert_eq!(
            constellation_meta::store::parse_quota_record(&value).unwrap(),
            Some(4096)
        );
        assert!(is_internal_xattr(SUBTREE_QUOTA_XATTR));
        assert!(!is_internal_xattr("user.constellation.csi.pv"));
        let policy = constellation_vfs::policy::XattrPolicy::linux();
        let root = constellation_vfs::Caller::with_groups(0, 0, &[0]);
        assert_eq!(
            policy.check_name(
                constellation_vfs::XattrName::new(SUBTREE_QUOTA_XATTR.as_bytes()),
                &root
            ),
            Err(Code::NotSupported)
        );
    }

    /// A view mounted at the capped directory is held to the cap; a
    /// whole-filesystem view is not (module docs).
    #[test]
    fn a_view_at_the_subtree_enforces_its_cap() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (pv, f) = volume(&meta, 40);
        let (whole, _d1) = test_fs(meta.clone());
        whole.set_subtree_quota(pv, Some(100)).unwrap();
        let (mut scoped, _d2) = test_fs(meta.clone());
        scoped.set_subtree_root("/volumes/pv").unwrap();
        assert_eq!(scoped.cached_subtree_quota(), Some(100));
        assert!(scoped.quota_check(f, 100).is_ok());
        assert_eq!(scoped.quota_check(f, 101).unwrap_err(), Code::NoSpace);
        assert!(whole.quota_check(f, 1 << 30).is_ok());
        assert_eq!(whole.cached_subtree_quota(), None);

        // Raising the cap (ControllerExpandVolume) shows once the cache is
        // invalidated, as `quota.set` does.
        whole.set_subtree_quota(pv, Some(1000)).unwrap();
        View::invalidate_quota_cache(&scoped.subtree_quota_cache);
        assert!(scoped.quota_check(f, 1000).is_ok());
    }

    /// The reviewer's case: cap 100, the walk taken at 0, two new 100-byte
    /// files inside one TTL. The cached walk alone would admit both (200
    /// under a 100 cap); the running delta refuses the second.
    #[test]
    fn two_files_in_one_ttl_cannot_both_fill_the_cap() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (pv, _) = volume(&meta, 0);
        let a = meta.create(pv, "a", 0o644, 0, 0).unwrap().ino;
        let b = meta.create(pv, "b", 0o644, 0, 0).unwrap().ino;
        let (whole, _d1) = test_fs(meta.clone());
        whole.set_subtree_quota(pv, Some(100)).unwrap();
        let (mut scoped, _d2) = test_fs(meta.clone());
        scoped.set_subtree_root("/volumes/pv").unwrap();
        scoped.statfs_ttl = Duration::from_secs(60);

        assert!(scoped.quota_check(a, 100).is_ok());
        meta.setattr(a, None, None, None, Some(100), None, None)
            .unwrap();
        assert_eq!(scoped.quota_check(b, 100).unwrap_err(), Code::NoSpace);
        assert_eq!(scoped.quota_check(b, 1).unwrap_err(), Code::NoSpace);
        // Rewriting `a` within its admitted length costs nothing more.
        assert!(scoped.quota_check(a, 100).is_ok());
        assert_eq!(scoped.subtree_walks(), 1, "one walk, on the first write");
    }

    /// Successive writes on one handle carry the whole intended length:
    /// charged once, not per call, whether or not the size has committed.
    #[test]
    fn growth_of_one_file_is_charged_once() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (pv, f) = volume(&meta, 10);
        let (whole, _d1) = test_fs(meta.clone());
        whole.set_subtree_quota(pv, Some(100)).unwrap();
        let (mut scoped, _d2) = test_fs(meta.clone());
        scoped.set_subtree_root("/volumes/pv").unwrap();
        scoped.statfs_ttl = Duration::from_secs(60);
        for len in [20, 50, 100] {
            assert!(scoped.quota_check(f, len).is_ok(), "{len}");
        }
        meta.setattr(f, None, None, None, Some(60), None, None)
            .unwrap();
        assert!(scoped.quota_check(f, 100).is_ok());
        assert_eq!(scoped.quota_check(f, 101).unwrap_err(), Code::NoSpace);
    }

    /// Freed space comes back through the re-walk a refusal may take once
    /// the walk is older than the TTL; a view far from its cap never walks
    /// again however many writes it admits.
    #[test]
    fn deletions_are_credited_by_the_rewalk_near_the_cap() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (pv, f) = volume(&meta, 0);
        let g = meta.create(pv, "g", 0o644, 0, 0).unwrap().ino;
        let (whole, _d1) = test_fs(meta.clone());
        whole.set_subtree_quota(pv, Some(1000)).unwrap();
        let (mut scoped, _d2) = test_fs(meta.clone());
        scoped.set_subtree_root("/volumes/pv").unwrap();
        scoped.statfs_ttl = Duration::from_secs(60);
        for len in (1..=900).step_by(9) {
            assert!(scoped.quota_check(f, len).is_ok());
        }
        assert_eq!(scoped.subtree_walks(), 1);
        meta.setattr(f, None, None, None, Some(900), None, None)
            .unwrap();
        assert_eq!(scoped.quota_check(g, 200).unwrap_err(), Code::NoSpace);
        assert_eq!(scoped.subtree_walks(), 1, "a fresh walk is not retaken");
        // The tenant deletes; once the walk is stale the next refusal
        // re-walks and finds the room.
        meta.setattr(f, None, None, None, Some(0), None, None)
            .unwrap();
        scoped.statfs_ttl = Duration::ZERO;
        assert!(scoped.quota_check(g, 200).is_ok());
        assert_eq!(scoped.subtree_walks(), 2);
    }

    /// Past [`GROWN_LIMIT`] inodes the delta over-counts (never under) and
    /// lets the next refusal re-walk without waiting for the TTL.
    #[test]
    fn a_dropped_delta_overcounts_and_rewalks() {
        let mut u = SubtreeUsage::default();
        u.rewalk(0);
        for ino in 0..GROWN_LIMIT as u64 {
            let extra = u.extra(1000 + ino, 0, 1);
            u.admit(1000 + ino, 0, 1, extra);
        }
        assert!(!u.may_rewalk(Duration::from_secs(60)));
        let extra = u.extra(1000, 0, 5);
        assert_eq!(extra, 4);
        u.admit(1000, 0, 5, extra);
        let extra = u.extra(5, 0, 1);
        u.admit(5, 0, 1, extra);
        assert!(u.imprecise);
        // Inode 1000, dropped, is charged again from its committed size.
        assert_eq!(u.extra(1000, 0, 5), 5);
        assert_eq!(u.used(), GROWN_LIMIT as u64 + 5);
        assert!(u.may_rewalk(Duration::from_secs(60)));
    }

    #[test]
    fn statfs_size_is_the_tighter_of_the_two_caps() {
        let block = 131072u64;
        // No subtree cap: exactly `statfs_blocks`.
        assert_eq!(
            statfs_blocks_capped(10 * block, 40 * block, Some(100 * block), None, block),
            statfs_blocks(10 * block, 40 * block, Some(100 * block), block)
        );
        // A 20-block volume holding 10: 10 free, 20 total, though the
        // filesystem has 60 to spare.
        assert_eq!(
            statfs_blocks_capped(
                10 * block,
                40 * block,
                Some(100 * block),
                Some(20 * block),
                block
            ),
            (20, 10)
        );
        // The filesystem runs out first.
        assert_eq!(
            statfs_blocks_capped(
                10 * block,
                95 * block,
                Some(100 * block),
                Some(50 * block),
                block
            ),
            (15, 5)
        );
        // Uncapped filesystem, capped volume over its cap: full, not negative.
        assert_eq!(
            statfs_blocks_capped(30 * block, 30 * block, None, Some(20 * block), block),
            (30, 0)
        );
    }
}
