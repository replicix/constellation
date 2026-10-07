// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::state::CompactionState,
    memtable::Memtable,
    tree::sealed::SealedMemtables,
    version::{persist_version, Version},
    SeqNo, SequenceNumberCounter,
};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{Arc, MutexGuard, RwLock},
};

/// A super version is a point-in-time snapshot of memtables and a [`Version`] (list of disk files)
#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable that is being written to
    #[doc(hidden)]
    pub active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) version: Version,

    pub(crate) seqno: SeqNo,
}

pub struct SuperVersions(VecDeque<SuperVersion>);

impl SuperVersions {
    pub fn new(version: Version) -> Self {
        Self(
            vec![SuperVersion {
                active_memtable: Arc::new(Memtable::new(0)),
                sealed_memtables: Arc::default(),
                version,
                seqno: 0,
            }]
            .into(),
        )
    }

    pub fn memtable_size_sum(&self) -> u64 {
        let mut set = crate::HashMap::default();

        for super_version in &self.0 {
            set.entry(super_version.active_memtable.id)
                .and_modify(|bytes| *bytes += super_version.active_memtable.size())
                .or_insert_with(|| super_version.active_memtable.size());

            for sealed in super_version.sealed_memtables.iter() {
                set.entry(sealed.id)
                    .and_modify(|bytes| *bytes += sealed.size())
                    .or_insert_with(|| sealed.size());
            }
        }

        set.into_values().sum()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn free_list_len(&self) -> usize {
        self.len().saturating_sub(1)
    }

    pub fn maintenance(&mut self, folder: &Path, gc_watermark: SeqNo) -> crate::Result<()> {
        self.take_garbage(folder, gc_watermark).remove()
    }

    /// CONSTELLATION PATCH: the in-memory half of [`Self::maintenance`].
    /// Takes out the versions no snapshot can see any more; they are
    /// dropped and their files removed by [`StaleVersions::remove`], once
    /// the version lock is released. Dropping a version can be disk I/O:
    /// the last version referencing a compaction's input tables deletes
    /// their files.
    #[doc(hidden)]
    pub fn take_garbage(&mut self, folder: &Path, gc_watermark: SeqNo) -> StaleVersions {
        let mut stale = StaleVersions(Vec::new());

        if gc_watermark == 0 {
            return stale;
        }

        if self.free_list_len() < 1 {
            return stale;
        }

        log::trace!("Running manifest GC with watermark={gc_watermark}");

        if let Some(hi_idx) = self.0.iter().rposition(|x| x.seqno < gc_watermark) {
            for _ in 0..hi_idx {
                let Some(head) = self.0.pop_front() else {
                    break;
                };

                log::trace!(
                    "Removing version #{} (seqno={})",
                    head.version.id(),
                    head.seqno,
                );

                let path = folder.join(format!("v{}", head.version.id()));
                stale.0.push((path, head));
            }
        }

        log::trace!("Manifest GC done, version length now {}", self.0.len());

        stale
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk.
    pub(crate) fn upgrade_version<F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>>(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: &SequenceNumberCounter,
        visible_seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        self.upgrade_version_with_seqno(tree_path, f, seqno.next(), visible_seqno)
    }

    /// Like `upgrade_version`, but takes an already-allocated sequence number.
    ///
    /// This is useful when the seqno must be coordinated with other operations
    /// (e.g., bulk ingestion where tables are recovered with the same seqno).
    pub(crate) fn upgrade_version_with_seqno<
        F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>,
    >(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: SeqNo,
        visible_seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        let mut next_version = f(&self.latest_version())?;
        next_version.seqno = seqno;
        log::trace!("Next version seqno={}", next_version.seqno);

        persist_version(tree_path, &next_version.version)?;
        self.append_version(next_version);

        visible_seqno.fetch_max(seqno + 1);

        Ok(())
    }

    /// CONSTELLATION PATCH: a version change that keeps disk I/O out of
    /// the version lock.
    ///
    /// Upstream's [`Self::upgrade_version`] runs under the version history's
    /// write lock, and with it [`persist_version`]: the new version file's
    /// fsync, the directory's, and the atomic rewrite of `current`. Every
    /// memtable insert takes that lock for reading (`Tree::append_entry`),
    /// so each flush and each compaction stalled every write to the tree
    /// (and, in fjall, every commit of the database, since a commit holds
    /// the journal's lock across its inserts) for as long as the disk took.
    ///
    /// Here the next version is computed from the latest one and persisted
    /// with no lock on the history held, then installed under a short write
    /// lock. Version changes are serialized by the tree's compaction state
    /// mutex instead (the `_serialized` guard: every caller of this function
    /// holds it, and so do the ones still on [`Self::upgrade_version`],
    /// ingestion), so the latest on-disk version cannot change in between and
    /// versions reach disk in the order they are installed. What can change
    /// in between is the memtables (a rotation seals the active one,
    /// inserts land in it): the installed super version is the latest one
    /// at install time with the new on-disk version, plus `memtables`
    /// applied to it.
    ///
    /// Crash safety is unchanged: `current` names the new version only
    /// after its file is synced, exactly as before; the in-memory switch
    /// comes later, and until then readers keep the previous version,
    /// whose tables are deleted only after the switch. Files of versions
    /// garbage-collected here are removed after the lock is released; one
    /// left behind by a crash is an orphan the next recovery removes.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn upgrade_version_unlocked(
        history: &RwLock<SuperVersions>,
        _serialized: &MutexGuard<'_, CompactionState>,
        tree_path: &Path,
        next_version: impl FnOnce(&SuperVersion) -> crate::Result<Version>,
        memtables: impl FnOnce(&mut SuperVersion),
        seqno: &SequenceNumberCounter,
        visible_seqno: &SequenceNumberCounter,
        gc_watermark: SeqNo,
    ) -> crate::Result<StaleVersions> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let base = history.read().expect("lock is poisoned").latest_version();

        let version = next_version(&base)?;
        let seqno = seqno.next();
        log::trace!("Next version seqno={seqno}");

        persist_version(tree_path, &version)?;

        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut history = history.write().expect("lock is poisoned");
        let mut next = history.latest_version();
        assert_eq!(
            next.version.id(),
            base.version.id(),
            "a version change was not serialized by the compaction state lock",
        );
        next.version = version;
        next.seqno = seqno;
        memtables(&mut next);
        history.append_version(next);

        visible_seqno.fetch_max(seqno + 1);

        Ok(history.take_garbage(tree_path, gc_watermark))
    }

    pub fn append_version(&mut self, version: SuperVersion) {
        self.0.push_back(version);
    }

    pub fn replace_latest_version(&mut self, version: SuperVersion) {
        if self.0.pop_back().is_some() {
            self.0.push_back(version);
        }
    }

    pub fn latest_version(&self) -> SuperVersion {
        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        self.0
            .iter()
            .last()
            .cloned()
            .expect("should always have a SuperVersion")
    }

    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
        if seqno == 0 {
            #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
            return self
                .0
                .front()
                .cloned()
                .expect("should always find a SuperVersion");
        }

        let version = self
            .0
            .iter()
            .rev()
            .find(|version| version.seqno < seqno)
            .cloned();

        if version.is_none() {
            log::error!("Failed to find a SuperVersion for snapshot with seqno={seqno}");
            log::error!("SuperVersions:");

            for version in self.0.iter().rev() {
                log::error!("-> {}, seqno={}", version.version.id(), version.seqno);
            }
        }

        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        version.expect("should always find a SuperVersion")
    }
}

/// CONSTELLATION PATCH: versions no snapshot can see any more, with their
/// files ([`SuperVersions::take_garbage`]). Dropped (or [`Self::remove`]d)
/// after the version lock is released.
#[doc(hidden)]
#[must_use]
pub struct StaleVersions(Vec<(PathBuf, SuperVersion)>);

impl StaleVersions {
    /// Drops the versions, then removes their files.
    pub fn remove(self) -> crate::Result<()> {
        for (path, version) in self.0 {
            drop(version);
            if path.try_exists()? {
                crate::file::retry_transient_io(|| std::fs::remove_file(&path))?;
            }
        }
        Ok(())
    }
}

/// CONSTELLATION PATCH: an insert never waits for a flush or a compaction
/// persisting its version (`SuperVersions::upgrade_version_unlocked`).
#[cfg(test)]
mod version_lock_tests {
    use crate::{version::persist::SLOW_PERSIST, AbstractTree, Config, SequenceNumberCounter};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const PERSIST_DELAY: Duration = Duration::from_millis(400);

    /// Runs `background` while inserting into `tree` (keys counted by
    /// `live`); returns the longest insert.
    fn longest_insert_during(
        tree: &crate::AnyTree,
        seqno: &SequenceNumberCounter,
        live: &mut u64,
        background: impl FnOnce(crate::AnyTree) -> crate::Result<()> + Send + 'static,
    ) -> Duration {
        let done = Arc::new(AtomicBool::new(false));
        let worker = {
            let (tree, done) = (tree.clone(), done.clone());
            std::thread::spawn(move || {
                let result = background(tree);
                done.store(true, Ordering::Release);
                result
            })
        };
        let mut longest = Duration::ZERO;
        while !done.load(Ordering::Acquire) {
            let started = Instant::now();
            tree.insert(format!("live-{live:08}"), "v", seqno.next());
            longest = longest.max(started.elapsed());
            *live += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
        #[expect(clippy::unwrap_used)]
        worker.join().unwrap().unwrap();
        longest
    }

    #[test]
    fn inserts_never_wait_for_a_version_being_persisted() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let seqno = SequenceNumberCounter::default();
        let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default()).open()?;

        for batch in 0..4u64 {
            for i in 0..1_000u64 {
                tree.insert(format!("k-{batch}-{i:05}"), "value", seqno.next());
            }
            tree.flush_active_memtable(0)?;
        }
        for i in 0..1_000u64 {
            tree.insert(format!("k-sealed-{i:05}"), "value", seqno.next());
        }

        *SLOW_PERSIST
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((folder.path().to_path_buf(), PERSIST_DELAY));

        let mut live = 0;
        let flush = longest_insert_during(&tree, &seqno, &mut live, |tree| {
            tree.flush_active_memtable(0)
        });
        let compaction = longest_insert_during(&tree, &seqno, &mut live, |tree| {
            tree.major_compact(u64::MAX, 0)
        });

        *SLOW_PERSIST
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;

        assert_eq!(1, tree.table_count());
        assert!(
            flush < PERSIST_DELAY / 2,
            "an insert waited {flush:?} for a flush persisting its version",
        );
        assert!(
            compaction < PERSIST_DELAY / 2,
            "an insert waited {compaction:?} for a compaction persisting its version",
        );
        assert_eq!(5_000 + live as usize, tree.len(crate::SeqNo::MAX, None)?);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn super_version_gc_above_watermark() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 0)?;

        assert_eq!(history.free_list_len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 1);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple_2() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_keep() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_shadowed() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 1);

        Ok(())
    }
}
