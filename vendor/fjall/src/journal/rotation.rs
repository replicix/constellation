// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): a journal
// rotation that never syncs under the journal writer's lock.
//
// Upstream rotated the journal with the writer's mutex held across three
// fsyncs: the sealed journal's `sync_all`, the new file's `sync_all` after
// its pre-allocation, and the folder's. Every commit takes that mutex, so
// every commit waited for those fsyncs once per rotation. Now:
//
// 1. The next journal file is created, pre-allocated and synced, and the
//    folder synced, before the lock is taken (`Writer::create_new`).
// 2. Under the lock, the sealed journal's buffer is written out (to the
//    OS, as every commit's is), the writer swaps to the next file, and the
//    next file starts with a *rotation marker*: an empty batch whose seqno
//    is the last one written to the sealed journal. No fsync.
// 3. The sealed journal is synced after the lock is released. Until that
//    sync is done, a durable persist (`SyncAll`, `SyncData`) of the new
//    journal also waits for it (`Seal`): a write acknowledged as durable
//    after the swap implies the sealed journal is durable too.
//
// Non-durable commits (no persist, or `PersistMode::Buffer`) reach the OS
// at once, as before, so a process crash loses nothing. What the marker is
// for is a power loss inside step 3: the kernel may write back pages of the
// new journal before the sealed journal's tail. Recovery then finds the
// marker's seqno missing from the end of the sealed journal, and discards
// the new journal and every later one (`RotationChain`), so what it
// recovers is still a prefix of the commits. Nothing in the discarded
// journals was acknowledged as durable: that would have waited for the
// sealed journal's sync, which would have made its tail, and so the
// marker's seqno, durable.
//
// A restart must not reopen that window: a sealed journal's sync may still
// be pending when the process dies (or, before the swap, the journal being
// sealed may hold unsynced writes while the next file is already there).
// Its tail is then in the page cache, so recovery reads it whole and finds
// the chain intact, but nothing in the new process would ever wait for its
// sync, and a power loss after a durable persist there would cut the chain
// below that persist. So recovery syncs every sealed journal before it
// reads them (`sync_sealed_journals`), as it syncs the active one.
//
// The marker is a batch fjall's journal format already has (`Start` with an
// item count of 0, then `End` with the checksum of nothing); upstream's
// recovery replays it as a batch with no items.

use super::{batch_reader::Batch, writer::Writer};
use crate::{file::fsync_directory, supervisor::Supervisor};
use lsm_tree::SeqNo;
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, PoisonError},
};

/// Rotates the journal if it outgrew the threshold, and returns once every
/// sealed journal is synced. A flush worker calls this before each flush:
/// upstream checked the threshold under the journal lock, so a flush also
/// started only after any rotation's syncs, and it still does (a worker
/// waits here, a commit does not).
pub fn rotate_if_full(supervisor: &Supervisor) -> crate::Result<()> {
    rotate(supervisor)?;
    supervisor.journal.wait_for_sealed()
}

fn rotate(supervisor: &Supervisor) -> crate::Result<()> {
    let journal = &supervisor.journal;
    let rotation = journal.lock_rotation();

    let next_path = {
        log::trace!("acquiring journal lock to maybe rotate journal");
        let mut journal_writer = journal.get_writer()?;
        if journal_writer.pos()? <= rotation_threshold(&journal_writer.path) {
            return Ok(());
        }
        journal_writer.next_path()?
    };

    // 1. The next file, outside the lock.
    let next = Writer::create_new(&next_path)?;
    #[expect(clippy::expect_used)]
    let folder = next_path.parent().expect("should have parent");
    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder)?;
    after_sync(folder);
    at(RotationPoint::Prepared, &next_path);

    // 2. The swap, under the lock, as upstream's rotation (same lock order).
    let sealed = {
        let mut journal_writer = journal.get_writer()?;

        #[expect(clippy::expect_used)]
        let mut journal_manager = supervisor
            .journal_manager
            .write()
            .expect("lock is poisoned");

        let seqno_map = {
            #[expect(clippy::expect_used)]
            let keyspaces = supervisor.keyspaces.write().expect("lock is poisoned");

            supervisor.build_seqno_map(&keyspaces)
        };

        let sealed = journal_manager.rotate_journal(&mut journal_writer, next, seqno_map)?;

        if journal_manager.disk_space_used() >= supervisor.db_config.max_journaling_size_in_bytes {
            let stragglers = journal_manager.get_keyspaces_to_flush_for_oldest_journal_eviction();

            for keyspace in stragglers {
                log::info!("Rotating {:?} to try to reduce journal size", keyspace.name);
                keyspace.request_rotation();
            }
        }

        sealed
    };
    drop(rotation);
    at(RotationPoint::Swapped, &next_path);

    // 3. The sealed journal's sync, outside the lock. The next rotation
    // need not wait for it: its seal is chained to this one.
    sealed.sync()?;
    at(RotationPoint::Sealed, &next_path);
    Ok(())
}

/// Upstream's rotation threshold: a flush rotates the journal once it is
/// larger than this.
const ROTATION_THRESHOLD: u64 = 64_000_000;

pub fn rotation_threshold(journal_path: &Path) -> u64 {
    #[cfg(test)]
    if let Some(threshold) = test_hooks::threshold(journal_path) {
        return threshold;
    }
    let _ = journal_path;
    ROTATION_THRESHOLD
}

/// Called right after every journal (or journal folder) sync: a test makes
/// it slow, or records what was synced.
pub fn after_sync(path: &Path) {
    #[cfg(test)]
    test_hooks::synced(path);
    let _ = path;
}

/// Recovery's sync of the sealed journals, oldest first, before they are
/// read: whatever a crashed process left unsynced in them (a rotation's
/// pending seal, or the journal being sealed when it died before the swap)
/// is durable before the new process acknowledges anything as durable
/// after it. Their folder entries are durable already: a journal file is
/// created, and its folder synced, before anything is written to it.
pub fn sync_sealed_journals<'a>(paths: impl IntoIterator<Item = &'a PathBuf>) -> crate::Result<()> {
    for path in paths {
        File::open(path)?.sync_all().inspect_err(|e| {
            log::error!(
                "Failed to fsync sealed journal file at {}: {e:?}",
                path.display(),
            );
        })?;
        after_sync(path);
    }
    Ok(())
}

/// Where a rotation is (test hooks pause or snapshot the database there).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RotationPoint {
    /// The next journal file exists (pre-allocated, synced); nothing is
    /// written to it yet.
    Prepared,
    /// The writer writes to the next journal; the sealed one is not synced.
    Swapped,
    /// The sealed journal is synced.
    Sealed,
}

pub fn at(point: RotationPoint, journal_path: &Path) {
    #[cfg(test)]
    test_hooks::at(point, journal_path);
    let _ = (point, journal_path);
}

/// The sealed journal's sync, which a durable persist after the swap waits
/// for, chained to the seal of the journal sealed before it while that one
/// is pending: a rotation does not wait for the previous sealed journal's
/// sync (holding the rotation back for it would let the active journal grow
/// past the threshold by everything written meanwhile).
#[derive(Default)]
pub struct Seal {
    /// `None` while the sync runs; then its outcome (an error's kind and
    /// text, since `io::Error` is not `Clone`).
    done: Mutex<Option<Result<(), (std::io::ErrorKind, String)>>>,
    cv: Condvar,
    /// The previous sealed journal's seal, until it succeeded.
    prev: Mutex<Option<Arc<Self>>>,
}

impl Seal {
    pub(crate) fn after(prev: Option<Arc<Self>>) -> Self {
        Self {
            prev: Mutex::new(prev),
            ..Self::default()
        }
    }

    fn complete(&self, result: Result<(), (std::io::ErrorKind, String)>) {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        if done.is_none() {
            *done = Some(result);
            self.cv.notify_all();
        }
    }

    fn prev(&self) -> Option<Arc<Self>> {
        self.prev
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether this sync and every earlier pending one finished and
    /// succeeded (a failed one is kept, so every later durable persist
    /// fails too).
    pub(crate) fn succeeded(&self) -> bool {
        if !matches!(
            *self.done.lock().unwrap_or_else(PoisonError::into_inner),
            Some(Ok(()))
        ) {
            return false;
        }
        match self.prev() {
            Some(prev) if !prev.succeeded() => false,
            Some(_) => {
                *self.prev.lock().unwrap_or_else(PoisonError::into_inner) = None;
                true
            }
            None => true,
        }
    }

    /// Waits for this sealed journal's sync and every earlier pending one.
    pub(crate) fn wait(&self) -> std::io::Result<()> {
        {
            let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                match &*done {
                    Some(Ok(())) => break,
                    Some(Err((kind, text))) => {
                        return Err(std::io::Error::new(
                            *kind,
                            format!("sealed journal sync failed: {text}"),
                        ))
                    }
                    None => done = self.cv.wait(done).unwrap_or_else(PoisonError::into_inner),
                }
            }
        }
        if let Some(prev) = self.prev() {
            prev.wait()?;
            *self.prev.lock().unwrap_or_else(PoisonError::into_inner) = None;
        }
        Ok(())
    }
}

/// A journal the writer no longer writes to, whose sync is still owed.
/// Dropping it without [`Self::sync`] fails its seal, so nobody waits
/// forever.
pub struct SealedJournal {
    pub(crate) file: File,
    pub(crate) path: PathBuf,
    pub(crate) seal: Arc<Seal>,
}

impl SealedJournal {
    pub(crate) fn sync(self) -> crate::Result<()> {
        let result = self.file.sync_all().inspect_err(|e| {
            log::error!(
                "Failed to fsync sealed journal file at {}: {e:?}",
                self.path.display(),
            );
        });
        after_sync(&self.path);
        self.seal.complete(match &result {
            Ok(()) => Ok(()),
            Err(e) => Err((e.kind(), e.to_string())),
        });
        result.map_err(Into::into)
    }
}

impl Drop for SealedJournal {
    fn drop(&mut self) {
        self.seal.complete(Err((
            std::io::ErrorKind::Other,
            String::from("rotation abandoned before the sealed journal was synced"),
        )));
    }
}

/// Recovery's check of the rotation markers, journal by journal from the
/// oldest: a journal whose first batch is a marker naming a seqno that its
/// predecessor does not end with was written while that predecessor's tail
/// was not durable, and is discarded with every later journal.
#[derive(Debug, Default)]
pub struct RotationChain {
    /// A journal was read before this one (the oldest journal has no
    /// predecessor left: it was flushed and deleted).
    has_predecessor: bool,
    /// The last batch seqno of the previous journal.
    prev_last: Option<SeqNo>,
    /// The last batch seqno of the journal being read.
    last: Option<SeqNo>,
    broken: bool,
}

impl RotationChain {
    /// Whether every journal from here on is discarded.
    pub(crate) fn is_broken(&self) -> bool {
        self.broken
    }

    /// Checks a journal's first batch; `false` if the journal (and every
    /// later one) is discarded.
    pub(crate) fn admits_first(&mut self, batch: &Batch) -> bool {
        let is_marker = batch.items.is_empty() && batch.cleared_keyspaces.is_empty();
        if is_marker && self.has_predecessor && self.prev_last != Some(batch.seqno) {
            log::warn!(
                "Journal rotation marker {} not at the end of the previous journal \
                 (which ends at {:?}): its tail was lost, discarding the journals after it",
                batch.seqno,
                self.prev_last,
            );
            self.broken = true;
        }
        !self.broken
    }

    pub(crate) fn note(&mut self, seqno: SeqNo) {
        self.last = Some(seqno);
    }

    /// A journal ended in a batch that does not read back whole (a
    /// checksum mismatch, a wrong item count): its last pages were lost, or
    /// it is damaged. Its whole batches before that one are replayed, and
    /// every later journal is discarded, whether or not it starts with a
    /// marker, so what recovery replays is still a prefix.
    pub(crate) fn torn(&mut self) {
        log::warn!(
            "Journal ends in a damaged batch after seqno {:?}: \
             discarding the journals after it",
            self.last,
        );
        self.broken = true;
    }

    pub(crate) fn end_journal(&mut self) {
        self.has_predecessor = true;
        self.prev_last = self.last.take();
    }

    /// Empties a discarded journal. This drops its pre-allocation too,
    /// which is fine for the active journal: its writer appends
    /// (`O_APPEND`), so it writes from offset 0 again, not after a hole.
    pub(crate) fn discard(path: &Path) -> crate::Result<()> {
        log::warn!("Discarding journal {}", path.display());
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_len(0)?;
        file.sync_all()?;
        after_sync(path);
        Ok(())
    }
}

/// Test-only knobs, keyed by database folder (lib tests run in parallel in
/// one process).
#[cfg(test)]
#[expect(clippy::expect_used, reason = "test-only knobs")]
pub mod test_hooks {
    use super::RotationPoint;
    use std::{
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::Duration,
    };

    type Hook = Arc<dyn Fn(RotationPoint) + Send + Sync>;
    type SyncHook = Arc<dyn Fn(&Path) + Send + Sync>;

    #[derive(Default)]
    struct Knobs {
        threshold: Option<u64>,
        slow_sync: Option<Duration>,
        hook: Option<Hook>,
        sync_hook: Option<SyncHook>,
    }

    static KNOBS: Mutex<Vec<(PathBuf, Knobs)>> = Mutex::new(Vec::new());

    fn with<R>(dir: &Path, f: impl FnOnce(&mut Knobs) -> R) -> R {
        let mut knobs = KNOBS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, k)) = knobs.iter_mut().find(|(d, _)| d == dir) {
            return f(k);
        }
        knobs.push((dir.to_path_buf(), Knobs::default()));
        f(&mut knobs.last_mut().expect("just pushed").1)
    }

    fn lookup<R>(path: &Path, f: impl FnOnce(&Knobs) -> Option<R>) -> Option<R> {
        let knobs = KNOBS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        knobs
            .iter()
            .find(|(d, _)| path.starts_with(d))
            .and_then(|(_, k)| f(k))
    }

    /// Rotates the journals of the database in `dir` past `bytes`.
    pub fn set_threshold(dir: &Path, bytes: u64) {
        with(dir, |k| k.threshold = Some(bytes));
    }

    /// Every journal sync under `dir` takes `delay` longer.
    pub fn set_slow_sync(dir: &Path, delay: Duration) {
        with(dir, |k| k.slow_sync = Some(delay));
    }

    pub fn set_hook(dir: &Path, hook: impl Fn(RotationPoint) + Send + Sync + 'static) {
        with(dir, |k| k.hook = Some(Arc::new(hook)));
    }

    /// `hook` is called with the path of every journal file (or folder)
    /// synced under `dir`, right after the sync.
    pub fn set_sync_hook(dir: &Path, hook: impl Fn(&Path) + Send + Sync + 'static) {
        with(dir, |k| k.sync_hook = Some(Arc::new(hook)));
    }

    pub fn clear(dir: &Path) {
        KNOBS
            .lock()
            .expect("lock is poisoned")
            .retain(|(d, _)| d != dir);
    }

    pub(super) fn threshold(path: &Path) -> Option<u64> {
        lookup(path, |k| k.threshold)
    }

    pub(super) fn synced(path: &Path) {
        if let Some(hook) = lookup(path, |k| k.sync_hook.clone()) {
            hook(path);
        }
        if let Some(delay) = lookup(path, |k| k.slow_sync) {
            std::thread::sleep(delay);
        }
    }

    pub(super) fn at(point: RotationPoint, path: &Path) {
        if let Some(hook) = lookup(path, |k| k.hook.clone()) {
            hook(point);
        }
    }
}
