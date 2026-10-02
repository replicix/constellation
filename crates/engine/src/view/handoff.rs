//! A view's state across a session handover (plan 31 §6.11):
//! [`HandleTableSnapshot`], [`View::export_handles`] and the import that
//! [`crate::Engine::open_view_resumed`] runs.
//!
//! What has to cross is small, because almost nothing a mounted view
//! knows lives only in the view. Inode numbers are the replica's (`Meta`,
//! on disk): the kernel's cached dentries and its lookup counts name
//! numbers the next process resolves the same way, and the view keeps no
//! lookup table (`forget` is a no-op). File handles are the view's own
//! numbering (one per open, plan 39 §3.7), so they cross as a table: each
//! handle's inode and the discard error event it has seen, plus the
//! view's count of handles per inode, which the last-close orphan reap and
//! the node's open-orphan holds read. Write sessions do not cross: a
//! detach publishes every one first (`Vfs::sync_view`), and a write on a
//! handed-over descriptor opens a fresh one from the published manifest.
//!
//! What does cross, then:
//!
//! - **`opens`**: handles per inode (the last close still reaps an
//!   unlinked-open orphan; the hold writer still claims it).
//! - **`handles`**: every handle the kernel holds, and the next number —
//!   without them every handed-over descriptor would read `EBADF`.
//! - **`errors`**: the node's discard error events (`LockTables`, plan 39
//!   §3.7) not yet forgotten — without them a description open at a
//!   discard would `fsync` to 0 in the next process. The import also
//!   raises the node's event numbering above every number that crossed,
//!   so a later event is newer than what each handle has seen.
//! - **`synthetic`**: the `.constellation` tree's numbering, which is the
//!   view's own (a counter above `SYNTHETIC_INO_BIT`, in lookup order) —
//!   without it every synthetic inode the kernel holds, and a snapshot
//!   view's *every* inode, would answer `ESTALE`.
//! - **`reached`**: a confined view's cache of inodes known inside it,
//!   so an inode it handed out and that was since renamed out of the
//!   subtree stays addressable by handle exactly as before.
//! - **`writers`**: the write-intent handles among `opens`, which refuse
//!   passthrough on their inode (plan 38 Z3b).
//! - **`passthrough`**: the chunk each passthrough handle sits on (plan 38
//!   §3(c)). A backing file registered with the kernel outlives the
//!   process that registered it — the kernel holds its own reference and
//!   keeps serving the handed-over descriptor from it — so the resumed
//!   view must hold the disk-cache pin that keeps the chunk where the
//!   kernel reads it. [`View::import_handles`] re-pins (and reopens) each
//!   one; the old process keeps its own pins until it `exec`s
//!   (`Engine::close_view_for_handover` leaves them), so the pin is never
//!   let go of before the new one exists except across the new image's
//!   own start-up. The backing *ids* are the FUSE session's, and cross in
//!   its own handoff (`constellation_frontend_fuse::FuseHandoff`).

use super::*;
use serde::{Deserialize, Serialize};

/// A view's open-handle table and numbering (see the module doc).
/// Serializable (postcard/JSON): it crosses processes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandleTableSnapshot {
    /// `(inode, open handles)`.
    pub opens: Vec<(Ino, u32)>,
    /// `(handle, its inode and seen error event)`, and the next number.
    pub handles: Vec<(u64, super::OpenHandle)>,
    pub handles_next: u64,
    /// `(inode, its latest discard error event)`, node-wide.
    pub errors: Vec<(Ino, u64)>,
    /// The confined view's reach cache.
    pub reached: Vec<Ino>,
    /// The synthetic registry: `(ino, key, node)` and the next number.
    synthetic: Vec<(Ino, String, SyntheticWire)>,
    synthetic_next: Ino,
    /// `(inode, write-intent handles)` among `opens`.
    pub writers: Vec<(Ino, u32)>,
    /// `(inode, chunk hashes)`: the chunk each passthrough handle on the
    /// inode reads from, oldest first.
    pub passthrough: Vec<(Ino, Vec<[u8; 32]>)>,
}

/// A synthetic node, opaquely (its type is the engine's own).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyntheticWire(SyntheticNode);

impl PartialEq for SyntheticWire {
    fn eq(&self, other: &Self) -> bool {
        // Structural equality through the wire form (tests only compare).
        serde_json::to_value(&self.0).ok() == serde_json::to_value(&other.0).ok()
    }
}

impl Eq for SyntheticWire {}

impl HandleTableSnapshot {
    /// The inodes with an open handle.
    pub fn open_inos(&self) -> impl Iterator<Item = Ino> + '_ {
        self.opens.iter().map(|(ino, _)| *ino)
    }
}

/// Everything a host needs to reopen a view in the next process
/// (`Engine::export_view`, `Engine::open_view_resumed`): the spec as it
/// resolves now — a writable clone's path rather than the selector it was
/// cloned from, with `ephemeral` (and `rw_snapshot: false`) meaning *that
/// path is a temporary clone, removed when the view closes* — and its
/// handle table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewHandoff {
    pub spec: ViewSpec,
    pub handles: HandleTableSnapshot,
}

impl View {
    /// This view's open handles and numbering (see the module doc). Take
    /// it once nothing reaches the view any more (after its session
    /// detached); `Engine::open_view_resumed` imports it.
    pub fn export_handles(&self) -> HandleTableSnapshot {
        let mut opens: Vec<(Ino, u32)> = self
            .opens
            .lock()
            .unwrap()
            .iter()
            .map(|(ino, n)| (*ino, *n))
            .collect();
        opens.sort_unstable();
        let mut reached = self.reach.all();
        reached.sort_unstable();
        reached.dedup();
        let registry = self.synthetic.lock().unwrap();
        let next = registry.next;
        let mut synthetic: Vec<(Ino, String, SyntheticWire)> = registry
            .keys
            .iter()
            .filter_map(|(key, ino)| {
                registry
                    .nodes
                    .get(ino)
                    .map(|node| (*ino, key.clone(), SyntheticWire(node.clone())))
            })
            .collect();
        synthetic.sort_by_key(|(ino, _, _)| *ino);
        drop(registry);
        let (handles, handles_next) = self.handles.export();
        let mut writers: Vec<(Ino, u32)> = self
            .writers
            .lock()
            .unwrap()
            .iter()
            .map(|(ino, n)| (*ino, *n))
            .collect();
        writers.sort_unstable();
        HandleTableSnapshot {
            opens,
            handles,
            handles_next,
            errors: self.meta.locks().export_errors(),
            reached,
            synthetic,
            synthetic_next: next,
            writers,
            passthrough: self.export_passthrough(),
        }
    }

    /// Adopt `snapshot` (a new view, before it serves anything and before
    /// its root is set: the root's synthetic key then finds its old
    /// number).
    pub(crate) fn import_handles(&self, snapshot: &HandleTableSnapshot) {
        {
            let mut opens = self.opens.lock().unwrap();
            for (ino, n) in &snapshot.opens {
                *opens.entry(*ino).or_insert(0) += n;
            }
        }
        self.handles
            .import(&snapshot.handles, snapshot.handles_next);
        let seen = snapshot.handles.iter().map(|(_, h)| h.seen).max();
        self.meta
            .locks()
            .import_errors(&snapshot.errors, seen.unwrap_or(0));
        {
            let mut writers = self.writers.lock().unwrap();
            for (ino, n) in &snapshot.writers {
                *writers.entry(*ino).or_insert(0) += n;
            }
        }
        self.import_passthrough(&snapshot.passthrough);
        for ino in &snapshot.reached {
            self.reach.mark(*ino);
        }
        let mut registry = self.synthetic.lock().unwrap();
        for (ino, key, node) in &snapshot.synthetic {
            registry.nodes.insert(*ino, node.0.clone());
            registry.keys.insert(key.clone(), *ino);
        }
        registry.next = registry.next.max(snapshot.synthetic_next);
    }

    /// Why this view's state cannot be handed over right now (empty: it
    /// can). A cluster lock held here lives in this node's memory
    /// (`Meta::locks`) and in its grant at the sequencer, neither of which
    /// a restart keeps — the application holding it would lose it
    /// silently — so a handover waits until none is held. (Under
    /// `--locks local` the kernel keeps every lock, and they cross by
    /// themselves.)
    pub fn handover_blockers(&self) -> Vec<String> {
        if self.cluster_locks().is_none() {
            return Vec::new();
        }
        let inos: Vec<Ino> = self.opens.lock().unwrap().keys().copied().collect();
        let mut out = Vec::new();
        for ino in inos {
            let held = self.meta.locks().local_locks(ino).len();
            if held > 0 {
                out.push(format!("inode {ino} holds {held} cluster lock(s)"));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_vfs::{Blocking, Name, OpCtx, OpKind, OpenOwner, Vfs, WriteData};

    fn cx(caller: &Caller, kind: OpKind) -> OpCtx<'_> {
        OpCtx::new(kind, caller)
    }

    /// Two views of one replica: the first process's, and the next one's.
    #[test]
    fn open_handles_and_synthetic_numbers_cross_a_handover() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (old, _d1) = super::super::quota_tests::test_fs(meta.clone());
        let caller = Caller::new(0, 0, None);
        let (kept, opened) = Blocking::run(|r| {
            old.create(
                &cx(&caller, OpKind::Create),
                ROOT_INO,
                Name::new("kept"),
                0o100644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        let ino = kept.attr.ino;
        Blocking::run(|r| {
            old.write(
                &cx(&caller, OpKind::Write),
                ino,
                opened.fh,
                0,
                WriteData::Borrowed(b"payload"),
                OpenFlags::WRITE,
                r,
            )
        })
        .unwrap();
        // Published (a dirty unlinked-open file cannot be published today:
        // `fsync`/`close` answer `ENOENT` — a pre-existing engine bug a
        // handover refuses on, see `cli/src/handover.rs`).
        Blocking::run(|r| {
            old.flush(
                &cx(&caller, OpKind::Flush),
                ino,
                opened.fh,
                constellation_vfs::LockOwner(0),
                r,
            )
        })
        .unwrap();
        // Unlinked while open: an orphan only the handle keeps.
        Blocking::run(|r| old.unlink(&cx(&caller, OpKind::Unlink), ROOT_INO, Name::new("kept"), r))
            .unwrap();
        let dot = Blocking::run(|r| {
            old.lookup(
                &cx(&caller, OpKind::Lookup),
                ROOT_INO,
                Name::new(".constellation"),
                r,
            )
        })
        .unwrap()
        .attr
        .ino;
        assert!(View::is_synthetic(dot));
        // The detach's barrier, then the export.
        Blocking::run(|r| old.sync_view(&cx(&caller, OpKind::SyncView), r)).unwrap();
        let snapshot = old.export_handles();
        assert_eq!(snapshot.opens, vec![(ino, 1)]);
        assert!(snapshot.open_inos().eq([ino]));

        // It crosses processes: both wire forms round-trip.
        let handoff = ViewHandoff {
            spec: ViewSpec::new("/"),
            handles: snapshot.clone(),
        };
        let json = serde_json::to_string(&handoff).unwrap();
        assert_eq!(serde_json::from_str::<ViewHandoff>(&json).unwrap(), handoff);
        let bytes = postcard::to_allocvec(&handoff).unwrap();
        assert_eq!(
            postcard::from_bytes::<ViewHandoff>(&bytes).unwrap(),
            handoff
        );

        let (new, _d2) = super::super::quota_tests::test_fs(meta.clone());
        new.import_handles(&snapshot);
        assert_eq!(new.export_handles(), snapshot);
        // The synthetic number the kernel holds resolves on the new view.
        let attr = Blocking::run(|r| new.getattr(&cx(&caller, OpKind::Getattr), dot, None, r))
            .expect("a synthetic inode handed over");
        assert_eq!(attr.kind, constellation_vfs::FileKind::Dir);
        // The handed-over handle's inode is still there for the new view
        // (its content lives in the node's cache and S3, which these two
        // test views do not share), and its last close reaps the orphan
        // on the new view.
        let attr =
            Blocking::run(|r| new.getattr(&cx(&caller, OpKind::Getattr), ino, Some(opened.fh), r))
                .unwrap();
        assert_eq!((attr.size, attr.nlink), (7, 0));
        Blocking::run(|r| {
            new.release(
                &cx(&caller, OpKind::Release),
                ino,
                opened.fh,
                OpenFlags::READ | OpenFlags::WRITE,
                None,
                r,
            )
        })
        .unwrap();
        assert!(new.export_handles().opens.is_empty());
        assert!(
            meta.getattr(ino).unwrap().is_none(),
            "the orphan was reaped at the last close, on the new view"
        );
    }

    /// Plan 39 §3.7: a discard error a handed-over description has not
    /// reported yet crosses into the next process (whose lock tables start
    /// empty), is reported there once, and an event noted there afterwards
    /// is newer than everything that crossed.
    #[test]
    fn an_unreported_discard_error_crosses_a_handover() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let file = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let (old, _d1) = super::super::quota_tests::test_fs(meta.clone());
        let caller = Caller::new(0, 0, None);
        let fh = Blocking::run(|r| {
            old.open(
                &cx(&caller, OpKind::Open),
                file.ino,
                OpenFlags::READ,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap()
        .fh;
        // Events on another inode first: the numbering is the node's.
        meta.locks().note_discard(file.ino + 1000);
        meta.locks().note_discard(file.ino + 1000);
        let seq = meta.locks().note_discard(file.ino);
        let snapshot = old.export_handles();
        assert!(snapshot.errors.contains(&(file.ino, seq)));

        // The next process: the same replica reopened, fresh lock tables.
        let next = Arc::new(Meta::open_in_memory().unwrap());
        let (new, _d2) = super::super::quota_tests::test_fs(next.clone());
        new.import_handles(&snapshot);
        assert_eq!(new.lock_publish_gate(file.ino, fh), Ok(true), "still owed");
        assert_eq!(new.lock_publish_gate(file.ino, fh), Ok(false), "once");
        assert!(next.locks().note_discard(file.ino) > seq, "numbered above");
        assert_eq!(new.lock_publish_gate(file.ino, fh), Ok(true), "a newer one");
    }

    #[test]
    fn an_empty_table_round_trips() {
        let empty = HandleTableSnapshot::default();
        let bytes = postcard::to_allocvec(&empty).unwrap();
        assert_eq!(
            postcard::from_bytes::<HandleTableSnapshot>(&bytes).unwrap(),
            empty
        );
    }
}
