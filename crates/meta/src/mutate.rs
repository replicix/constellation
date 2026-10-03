//! Forwarded mutations: the operation a non-holder sends to the lease
//! holder for authoritative validate-and-journal (see DESIGN.md §4/§5).
//!
//! Wire encoding is postcard; the net crate carries these as opaque
//! bytes so it stays free of the metadata dependency.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::Meta;
use crate::{MetaStore, SetXattrMode};
use constellation_fs_core::{Ino, InodeKind};
use constellation_types::{Code, Rdev};
use serde::{Deserialize, Serialize};

/// One mutation a requester asks the lease holder to execute.
///
/// Inode-creating variants carry the requester's already-allocated ino
/// (node-prefixed, cluster-unique) so the requester's kernel-visible
/// number is stable across the forward and a scratch `Publish` can
/// promote a local inode without remapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutateOp {
    Mkdir {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    Create {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    Symlink {
        parent: Ino,
        name: String,
        ino: Ino,
        target: String,
        uid: u32,
        gid: u32,
    },
    Mknod {
        parent: Ino,
        name: String,
        ino: Ino,
        kind: u8,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: Rdev,
    },
    Link {
        ino: Ino,
        parent: Ino,
        name: String,
    },
    Unlink {
        parent: Ino,
        name: String,
    },
    Rmdir {
        parent: Ino,
        name: String,
    },
    Rename {
        parent: Ino,
        name: String,
        new_parent: Ino,
        new_name: String,
        /// `renameat2(RENAME_NOREPLACE)`: refuse with `Exists` if
        /// `new_parent/new_name` exists. Checked where the rename commits
        /// (this node's own transaction, or the holder's for a forwarded
        /// op), never against a requester's possibly stale replica or a
        /// kernel's dcache, so two nodes racing to claim one name cannot
        /// both win. Journals as a plain `Rename` (the target was absent).
        noreplace: bool,
    },
    Setattr {
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    },
    SetManifest {
        ino: Ino,
        base_manifest: Option<Vec<u8>>,
        manifest: Vec<u8>,
        size: u64,
    },
    SetXattr {
        ino: Ino,
        name: String,
        value: Vec<u8>,
        /// 0 = Set, 1 = Create, 2 = Replace (matches [`SetXattrMode`]).
        mode: u8,
    },
    RemoveXattr {
        ino: Ino,
        name: String,
    },
    /// Batched read-time atime bumps forwarded to the holder (plan 20).
    /// Each entry is `(ino, atime_ns, time_ns)`; `time_ns` is the
    /// emitter's observation time, carried so the holder can apply the
    /// ctime guard against the emitter's clock rather than its own.
    /// Best effort: a holder applies and queues it for shipping, and
    /// appends nothing to the write journal. Appended last so a peer too
    /// old to decode it falls back to `Busy` (a harmless rotation).
    AtimeBatch {
        entries: Vec<(Ino, i64, i64)>,
    },
    /// Scratch → shared publish: create the inode at `parent/name` with
    /// the given attrs and commit its manifest (and any xattrs staged on
    /// the scratch file) in one transaction.
    Publish {
        ino: Ino,
        parent: Ino,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime_ns: i64,
        manifest: Vec<u8>,
        size: u64,
        xattrs: Vec<(String, Vec<u8>)>,
        /// `renameat2(RENAME_NOREPLACE)` of the scratch file: refuse
        /// with `Exists` if `parent/name` exists (see
        /// [`MutateOp::Rename::noreplace`]).
        noreplace: bool,
    },
    /// Plan 30 §M3b: re-apply already-decided records through the replay
    /// path and journal them — how a deposed holder's stranded
    /// transaction that had no op of its own (a snapshot row, a quota
    /// change, a clone) is replayed by rid on the current holder
    /// (`store::spec`'s `derive_replay_op`). The records apply with the
    /// same skip-on-conflict rules every tailing replica uses, so the
    /// result is whatever the log order makes of them.
    Records {
        records: Vec<LogRecord>,
    },
    /// `renameat2(RENAME_EXCHANGE)`: atomically swap the inodes
    /// `parent/name` and `new_parent/new_name` name (both must exist; any
    /// kinds; across directories too). Journals one
    /// [`LogRecord::Exchange`].
    Exchange {
        parent: Ino,
        name: String,
        new_parent: Ino,
        new_name: String,
    },
}

impl MutateOp {
    pub fn to_postcard(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    pub fn from_postcard(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Holder's answer, before it is packed into a wire `Payload`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutateOutcome {
    Accepted {
        epoch: u64,
        records: Vec<LogRecord>,
    },
    /// A definitive refusal. Plan 31 §7: the portable [`Code`], which
    /// crosses the P2P wire as its own number; a frontend converts it to
    /// its kernel's errno when it answers.
    Errno(Code),
    NotHolder {
        holder: u64,
    },
    Busy,
    /// The holder refused an optimistic whole-file manifest commit
    /// because its base is stale. Carries the manifest that *is*
    /// current, so the requester can rebase in the same round trip
    /// rather than wait for the holder's segment to ship.
    ///
    /// Appended last on purpose: a peer too old to decode this variant
    /// falls back to `Busy` (the documented handling of an undecodable
    /// outcome), which costs a lease rotation but stays correct.
    Conflict {
        manifest: Option<Vec<u8>>,
    },
    /// The holder refused a create-family op with `EEXIST`, and carries
    /// the entry that *is* there so the requester can install it in the
    /// same round trip.
    ///
    /// POSIX requires `mkdir`/`O_EXCL` on an existing name to fail, so
    /// the refusal itself stands (`mkdir` is a lock primitive). But the
    /// caller acts on that answer immediately — `mkdir -p` walks into
    /// the directory it was just told exists — and until the holder's
    /// segment reaches this replica, that lookup would fail with an
    /// `ENOENT` the holder never said. Replay is idempotent for an
    /// identical entry (`replay::insert_node` returns early when the
    /// dentry already names the same ino), so installing it early and
    /// applying the holder's segment later converge.
    ///
    /// Appended last, like `Conflict`: a peer too old to decode it falls
    /// back to `Busy`.
    Exists {
        records: Vec<LogRecord>,
        // Plan 30 §M6: the hint's floor is no longer carried here; the
        // reply's position (`PeerMsg::MutateReply::position`) gives it
        // (`Position::hint_floor`).
        /// The answering holder's epoch. Plan 30 §M3a: the installed
        /// entry is speculation (`Meta::install_hint`) and is rolled back
        /// if a segment from a later epoch reaches this replica before
        /// the floor does — the entry may have been this holder's own
        /// unshipped work, stranded with it.
        epoch: u64,
    },
    /// Plan 30 §M8: the holder executed the op (or found it executed),
    /// but its acknowledgement waits for the read delegations on what it
    /// touched to be recalled, or to expire (`cto=strict`'s close-to-open:
    /// no delegate may still serve the old state once this op completes).
    /// The requester retries the same rid after `retry_ms` without
    /// counting an attempt; the holder answers from its dedup once the
    /// wait is over. Never reaches a client.
    Held {
        retry_ms: u64,
    },
}

/// What a forwarded op's transaction waits for from the node that
/// forwarded it, as the sequencer answering it sees at reply time (chunk
/// close-stall-metered). A `--write-mode back` close forwards its
/// manifest with chunks still pending on the forwarder; the sequencer
/// enrolls them (`Meta::enroll_remote_chunks`) and ships — or, as a
/// delegate, streams — the close only once the forwarder reports them up,
/// and with it every later transaction that depends on it (a `chmod` or a
/// `rename` of the same file: `Meta::remote_blockers`). A forwarder that
/// must see its op's transaction (its reply came on state it has not
/// applied, or the acknowledgement waits for it) needs those chunks up,
/// and with its uploads held on a metered network nothing else would put
/// them there. Each variant but `None` names the inodes whose pending
/// chunks it is.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OwnChunks {
    /// Nothing of the forwarder's is pending in what the transaction
    /// waits for (or the outcome carries none, or the hold is not an
    /// acknowledgement's).
    #[default]
    None,
    /// It waits for the forwarder's pending chunks of these inodes, but
    /// the sequencer's pre-S3 stream carries it to the forwarder past
    /// them (a backed root, or a continuation epoch's hold owner, that it
    /// subscribes to, with nothing else's pending chunk in the way): the
    /// upload is not on the path.
    Streamed(Vec<Ino>),
    /// It waits for the forwarder's pending chunks of these inodes and
    /// nothing brings it back to the forwarder, or acknowledges it, until
    /// they are in S3: upload them now.
    Upload(Vec<Ino>),
}

impl OwnChunks {
    /// The P2P wire form (`constellation_net::Payload::MutateReply`'s
    /// `own_chunks` and `own_inos`).
    pub fn to_wire(&self) -> (u8, Vec<Ino>) {
        match self {
            OwnChunks::None => (0, Vec::new()),
            OwnChunks::Streamed(inos) => (1, inos.clone()),
            OwnChunks::Upload(inos) => (2, inos.clone()),
        }
    }

    /// An unknown value reads as [`OwnChunks::None`]: the forwarder then
    /// relies on its rounds, as before the signal.
    pub fn from_wire(v: u8, inos: Vec<Ino>) -> Self {
        match v {
            1 => OwnChunks::Streamed(inos),
            2 => OwnChunks::Upload(inos),
            _ => OwnChunks::None,
        }
    }

    /// The inodes whose pending chunks are named (empty for `None`).
    pub fn inos(&self) -> &[Ino] {
        match self {
            OwnChunks::None => &[],
            OwnChunks::Streamed(inos) | OwnChunks::Upload(inos) => inos,
        }
    }
}

impl MutateOutcome {
    pub fn to_postcard(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    pub fn from_postcard(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Execute `op` against the holder's authoritative replica and return
/// exactly the journal records its own transaction appended, as
/// `journal::append_tx` wrote them on this thread (`journal::OwnRows`) —
/// never another writer's rows committed while it ran. When `rid` is
/// given, `LogRecord::Completed { rid }` sits directly behind the op's
/// first record (plan 30 §M2): the pair gets consecutive seqs inside one
/// `SingleWriterWriteTx`, so a concurrent writer's rows can land in the
/// journal only entirely before or entirely after it. That adjacency is
/// what readers attribute a transaction by — see
/// `engine::coop::fresh::written_chunks`.
/// The caller is responsible for checking lease ownership first.
///
/// `rid` should be `Some` for every FUSE-issued mutation (local fast
/// path, holder-side forward execution, or a lease-path retry) and
/// `None` for op executions that are not part of the exactly-once
/// forwarding protocol at all (system-generated unlinks from retention
/// pruning, reintegration replay of already-decided records, tests that
/// do not exercise M2). The rid reaches `journal::append_tx` through a
/// thread-local set for the duration of this call
/// (`store::journal::PendingCompletion`), so the op's own transaction —
/// and only it — carries the completion marker.
pub fn execute(
    meta: &Meta,
    op: &MutateOp,
    rid: Option<crate::rid::Rid>,
) -> Result<Vec<LogRecord>, MetaError> {
    let _pending = crate::store::journal::PendingCompletion::set(rid);
    // Plan 30 §M3b: the op's own journaled transaction records `(rid, op)`
    // in its `journal_tx` row (`store::local`), so a deposition can replay
    // it by rid.
    let _op = crate::store::journal::PendingLocalOp::set(rid, op);
    crate::readdeleg::clear_victims();
    let records = execute_inner(meta, op)?;
    meta.note_unshipped(&records);
    Ok(records)
}

fn execute_inner(meta: &Meta, op: &MutateOp) -> Result<Vec<LogRecord>, MetaError> {
    // The op's outcome is the rows its own transaction appended, as
    // `journal::append_tx` wrote them on this thread — never a range of
    // the shared journal, which other writers append to (and ships
    // delete from) while the op runs (see `journal::OwnRows`).
    let own = crate::store::journal::OwnRows::collect();
    #[cfg(test)]
    window_hook::fire(meta, window_hook::At::Before);
    match op {
        MutateOp::Mkdir {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
        } => {
            meta.mkdir_at(*parent, name, *ino, *mode, *uid, *gid)?;
        }
        MutateOp::Create {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
        } => {
            meta.create_at(*parent, name, *ino, *mode, *uid, *gid)?;
        }
        MutateOp::Symlink {
            parent,
            name,
            ino,
            target,
            uid,
            gid,
        } => {
            meta.symlink_at(*parent, name, *ino, target, *uid, *gid)?;
        }
        MutateOp::Mknod {
            parent,
            name,
            ino,
            kind,
            mode,
            uid,
            gid,
            rdev,
        } => {
            let kind = InodeKind::from_u8(*kind)
                .ok_or_else(|| MetaError::Invalid(format!("unknown inode kind {kind}")))?;
            meta.mknod_at(*parent, name, *ino, kind, *mode, *uid, *gid, *rdev)?;
        }
        MutateOp::Link { ino, parent, name } => {
            meta.link(*ino, *parent, name)?;
        }
        MutateOp::Unlink { parent, name } => {
            meta.unlink(*parent, name)?;
        }
        MutateOp::Rmdir { parent, name } => {
            meta.rmdir(*parent, name)?;
        }
        MutateOp::Rename {
            parent,
            name,
            new_parent,
            new_name,
            noreplace: false,
        } => {
            meta.rename(*parent, name, *new_parent, new_name)?;
        }
        MutateOp::Rename {
            parent,
            name,
            new_parent,
            new_name,
            noreplace: true,
        } => {
            meta.rename_noreplace(*parent, name, *new_parent, new_name)?;
        }
        MutateOp::Exchange {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            meta.exchange(*parent, name, *new_parent, new_name)?;
        }
        MutateOp::Setattr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime_ns,
            mtime_ns,
        } => {
            meta.setattr(*ino, *mode, *uid, *gid, *size, *atime_ns, *mtime_ns)?;
        }
        MutateOp::SetManifest {
            ino,
            base_manifest,
            manifest,
            size,
        } => {
            meta.set_manifest_with_base(*ino, base_manifest.as_deref(), manifest, *size)?;
        }
        MutateOp::SetXattr {
            ino,
            name,
            value,
            mode,
        } => {
            let mode = match mode {
                1 => SetXattrMode::Create,
                2 => SetXattrMode::Replace,
                _ => SetXattrMode::Set,
            };
            meta.set_xattr(*ino, name, value, mode)?;
        }
        MutateOp::RemoveXattr { ino, name } => {
            meta.remove_xattr(*ino, name)?;
        }
        MutateOp::Publish {
            ino,
            parent,
            name,
            mode,
            uid,
            gid,
            mtime_ns,
            manifest,
            size,
            xattrs,
            noreplace,
        } => {
            meta.publish_file(
                *parent, name, *ino, *mode, *uid, *gid, *mtime_ns, manifest, *size, xattrs,
                *noreplace,
            )?;
        }
        MutateOp::Records { records } => {
            // A `Records` op is a stranded transaction re-applied by rid
            // (plan 30 §M3b's rid-less local work, or §M9's adopted
            // backup tail stranded again by a second failover): its
            // manifests may name chunks only a departed holder had, so
            // they are enrolled for the durability check exactly as an
            // adopted tail's are (`Meta::apply_adopted_records`).
            meta.apply_adopted_records(records, None)?;
        }
        MutateOp::AtimeBatch { entries } => {
            // Apply to the holder's own inode table (so its stat reflects
            // the read) and queue for shipping. Never writes the journal,
            // so the requester gets back an empty record set — it already
            // applied the bump locally before forwarding.
            meta.apply_atime(entries)?;
            meta.queue_atime(entries)?;
            return Ok(Vec::new());
        }
    }
    #[cfg(test)]
    window_hook::fire(meta, window_hook::At::After);
    let rows = own.take();
    // One transaction's rows: strictly increasing seqs (a seq seen twice
    // would be an aborted transaction's, reissued to the next).
    debug_assert!(rows.windows(2).all(|w| w[0].0 < w[1].0), "{rows:?}");
    Ok(rows.into_iter().map(|(_, r)| r).collect())
}

/// Test-only: a hook run on the executing thread right before the op's
/// transaction and right after it — where another writer's journal
/// commit can land while `execute` runs
/// (`tests::foreign_journal_rows_never_ride_a_forwarded_outcome`).
#[cfg(test)]
pub(crate) mod window_hook {
    use super::Meta;
    use std::cell::RefCell;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum At {
        Before,
        After,
    }

    type Hook = Box<dyn FnMut(&Meta, At)>;

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(crate) fn set(hook: Option<Hook>) {
        HOOK.with(|h| *h.borrow_mut() = hook);
    }

    pub(crate) fn fire(meta: &Meta, at: At) {
        let hook = HOOK.with(|h| h.borrow_mut().take());
        if let Some(mut hook) = hook {
            hook(meta, at);
            HOOK.with(|h| *h.borrow_mut() = Some(hook));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;

    fn rename_op(p: Ino, n: &str, np: Ino, nn: &str, noreplace: bool) -> MutateOp {
        MutateOp::Rename {
            parent: p,
            name: n.into(),
            new_parent: np,
            new_name: nn.into(),
            noreplace,
        }
    }

    fn exchange_op(p: Ino, n: &str, np: Ino, nn: &str) -> MutateOp {
        MutateOp::Exchange {
            parent: p,
            name: n.into(),
            new_parent: np,
            new_name: nn.into(),
        }
    }

    fn ino_at(m: &Meta, p: Ino, n: &str) -> Option<Ino> {
        m.lookup(p, n).unwrap().map(|a| a.ino)
    }

    /// A tailing replica of `m`: every record `m` journaled so far,
    /// applied through replay.
    fn replica_of(m: &Meta) -> Meta {
        let r = Meta::open_in_memory().unwrap();
        let records: Vec<LogRecord> = m
            .peek_journal_after(0)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        r.apply_records(&records).unwrap();
        r
    }

    #[test]
    fn rename_noreplace_refuses_an_existing_target_and_journals_a_plain_rename() {
        let m = Meta::open_in_memory().unwrap();
        let a = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let b = m.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        let before = m.journal_len().unwrap();
        let refused = execute(&m, &rename_op(ROOT_INO, "a", ROOT_INO, "b", true), None);
        assert_eq!(refused.unwrap_err().code(), Code::Exists);
        // Nothing moved, nothing was unlinked, nothing journaled.
        assert_eq!(ino_at(&m, ROOT_INO, "a"), Some(a.ino));
        assert_eq!(ino_at(&m, ROOT_INO, "b"), Some(b.ino));
        assert_eq!(m.journal_len().unwrap(), before);
        // Even onto the source's own name (Linux: `EEXIST` first).
        let own = execute(&m, &rename_op(ROOT_INO, "a", ROOT_INO, "a", true), None);
        assert_eq!(own.unwrap_err().code(), Code::Exists);
        // A free name: an ordinary rename.
        let records = execute(&m, &rename_op(ROOT_INO, "a", ROOT_INO, "c", true), None).unwrap();
        assert!(matches!(records.as_slice(), [LogRecord::Rename { .. }]));
        assert_eq!(ino_at(&m, ROOT_INO, "c"), Some(a.ino));
        assert_eq!(ino_at(&m, ROOT_INO, "a"), None);
        // The op crosses the wire with its flag.
        let op = rename_op(ROOT_INO, "c", ROOT_INO, "b", true);
        assert_eq!(
            MutateOp::from_postcard(&op.to_postcard().unwrap()).unwrap(),
            op
        );
    }

    #[test]
    fn exchange_swaps_two_files_across_directories_and_replays_identically() {
        let m = Meta::open_in_memory().unwrap();
        let d = m.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        let a = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let b = m.create(d.ino, "b", 0o644, 0, 0).unwrap();
        let root_before = m.getattr(ROOT_INO).unwrap().unwrap();
        let d_before = m.getattr(d.ino).unwrap().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let records = execute(&m, &exchange_op(ROOT_INO, "a", d.ino, "b"), None).unwrap();
        assert!(matches!(records.as_slice(), [LogRecord::Exchange { .. }]));
        assert_eq!(ino_at(&m, ROOT_INO, "a"), Some(b.ino));
        assert_eq!(ino_at(&m, d.ino, "b"), Some(a.ino));
        // Both inodes keep their link count and get a ctime; both parents
        // an mtime and ctime; no directory link moved.
        for (ino, was) in [(a.ino, &a), (b.ino, &b)] {
            let now = m.getattr(ino).unwrap().unwrap();
            assert_eq!(now.nlink, 1);
            assert!(now.ctime_ns > was.ctime_ns, "ctime of {ino:#x}");
            assert_eq!(now.mtime_ns, was.mtime_ns, "mtime of {ino:#x}");
        }
        for (ino, was) in [(ROOT_INO, &root_before), (d.ino, &d_before)] {
            let now = m.getattr(ino).unwrap().unwrap();
            assert_eq!(now.nlink, was.nlink);
            assert!(now.mtime_ns > was.mtime_ns && now.ctime_ns > was.ctime_ns);
        }
        // The dentry copies of the attributes follow the swap.
        assert_eq!(
            m.lookup(ROOT_INO, "a").unwrap().unwrap(),
            m.getattr(b.ino).unwrap().unwrap()
        );
        let r = replica_of(&m);
        for (p, n) in [(ROOT_INO, "a"), (d.ino, "b"), (ROOT_INO, "d")] {
            assert_eq!(
                r.lookup(p, n).unwrap(),
                m.lookup(p, n).unwrap(),
                "replica {p:#x}/{n}"
            );
        }
        assert_eq!(r.getattr(d.ino).unwrap(), m.getattr(d.ino).unwrap());
    }

    #[test]
    fn exchange_of_a_directory_and_a_file_across_parents_moves_the_dotdot_link() {
        let m = Meta::open_in_memory().unwrap();
        let p1 = m.mkdir(ROOT_INO, "p1", 0o755, 0, 0).unwrap();
        let p2 = m.mkdir(ROOT_INO, "p2", 0o755, 0, 0).unwrap();
        let s = m.mkdir(p1.ino, "s", 0o755, 0, 0).unwrap();
        m.create(s.ino, "inside", 0o644, 0, 0).unwrap();
        let f = m.create(p2.ino, "f", 0o644, 0, 0).unwrap();
        assert_eq!(m.getattr(p1.ino).unwrap().unwrap().nlink, 3);
        assert_eq!(m.getattr(p2.ino).unwrap().unwrap().nlink, 2);
        execute(&m, &exchange_op(p1.ino, "s", p2.ino, "f"), None).unwrap();
        let at_f = m.lookup(p2.ino, "f").unwrap().unwrap();
        assert_eq!((at_f.ino, at_f.kind), (s.ino, InodeKind::Dir));
        assert_eq!(ino_at(&m, p1.ino, "s"), Some(f.ino));
        assert_eq!(m.getattr(p1.ino).unwrap().unwrap().nlink, 2);
        assert_eq!(m.getattr(p2.ino).unwrap().unwrap().nlink, 3);
        // The directory kept its content, and its `..` is its new parent:
        // it can no longer be moved beneath p2 but can beneath p1.
        assert_eq!(m.readdir(s.ino).unwrap().len(), 1);
        let loop_ = execute(&m, &rename_op(p2.ino, "f", s.ino, "x", false), None);
        assert_eq!(loop_.unwrap_err().code(), Code::Invalid);
        // Two directories across parents: counts unchanged.
        let t = m.mkdir(p1.ino, "t", 0o755, 0, 0).unwrap();
        execute(&m, &exchange_op(p1.ino, "t", p2.ino, "f"), None).unwrap();
        assert_eq!(ino_at(&m, p1.ino, "t"), Some(s.ino));
        assert_eq!(ino_at(&m, p2.ino, "f"), Some(t.ino));
        assert_eq!(m.getattr(p1.ino).unwrap().unwrap().nlink, 3);
        assert_eq!(m.getattr(p2.ino).unwrap().unwrap().nlink, 3);
        let r = replica_of(&m);
        for ino in [p1.ino, p2.ino, s.ino, t.ino, f.ino] {
            assert_eq!(
                r.getattr(ino).unwrap().map(|a| (a.nlink, a.kind)),
                m.getattr(ino).unwrap().map(|a| (a.nlink, a.kind)),
                "replica {ino:#x}"
            );
        }
        assert_eq!(ino_at(&r, p1.ino, "t"), Some(s.ino));
        assert_eq!(ino_at(&r, p1.ino, "s"), Some(f.ino));
        assert_eq!(ino_at(&r, p2.ino, "f"), Some(t.ino));
    }

    #[test]
    fn exchange_refusals_and_no_ops() {
        let m = Meta::open_in_memory().unwrap();
        let d = m.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        let sub = m.mkdir(d.ino, "sub", 0o755, 0, 0).unwrap();
        let a = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        m.link(a.ino, ROOT_INO, "a2").unwrap();
        let before = m.journal_len().unwrap();
        let missing = execute(&m, &exchange_op(ROOT_INO, "a", ROOT_INO, "zz"), None);
        assert_eq!(missing.unwrap_err().code(), Code::NotFound);
        let missing = execute(&m, &exchange_op(ROOT_INO, "zz", ROOT_INO, "a"), None);
        assert_eq!(missing.unwrap_err().code(), Code::NotFound);
        // A directory swapped with its own descendant (either way round).
        let beneath = execute(&m, &exchange_op(ROOT_INO, "d", d.ino, "sub"), None);
        assert_eq!(beneath.unwrap_err().code(), Code::Invalid);
        let beneath = execute(&m, &exchange_op(d.ino, "sub", ROOT_INO, "d"), None);
        assert_eq!(beneath.unwrap_err().code(), Code::Invalid);
        // One name, or two names of one inode: nothing to do.
        assert!(
            execute(&m, &exchange_op(ROOT_INO, "a", ROOT_INO, "a"), None)
                .unwrap()
                .is_empty()
        );
        assert!(
            execute(&m, &exchange_op(ROOT_INO, "a", ROOT_INO, "a2"), None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(m.journal_len().unwrap(), before);
        assert_eq!(ino_at(&m, d.ino, "sub"), Some(sub.ino));
        let op = exchange_op(ROOT_INO, "a", d.ino, "sub");
        assert_eq!(
            MutateOp::from_postcard(&op.to_postcard().unwrap()).unwrap(),
            op
        );
    }

    #[test]
    fn publish_noreplace_refuses_another_inode_and_accepts_its_own_retry() {
        let m = Meta::open_in_memory().unwrap();
        let taken = m.create(ROOT_INO, "taken", 0o644, 0, 0).unwrap();
        let publish = |name: &str, ino: Ino| MutateOp::Publish {
            ino,
            parent: ROOT_INO,
            name: name.into(),
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns: 1,
            manifest: b"M".to_vec(),
            size: 1,
            xattrs: Vec::new(),
            noreplace: true,
        };
        let ino = (1 << 40) | 7;
        let refused = execute(&m, &publish("taken", ino), None).unwrap_err();
        assert_eq!(refused.code(), Code::Exists);
        assert_eq!(ino_at(&m, ROOT_INO, "taken"), Some(taken.ino));
        assert!(m.getattr(ino).unwrap().is_none());
        assert!(!execute(&m, &publish("free", ino), None).unwrap().is_empty());
        assert!(execute(&m, &publish("free", ino), None).unwrap().is_empty());
        assert_eq!(ino_at(&m, ROOT_INO, "free"), Some(ino));
    }

    #[test]
    fn exchange_replay_skips_when_an_entry_is_gone() {
        let m = Meta::open_in_memory().unwrap();
        m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let skipped = m
            .apply_foreign(
                &[LogRecord::Exchange {
                    parent: ROOT_INO,
                    name: "a".into(),
                    new_parent: ROOT_INO,
                    new_name: "gone".into(),
                    time_ns: 1,
                }],
                &crate::replay::TouchSet::default(),
            )
            .unwrap();
        assert_eq!(skipped, 1);
        assert!(m.lookup(ROOT_INO, "a").unwrap().is_some());
    }

    #[test]
    fn atime_batch_applies_locally_queues_for_ship_and_journals_nothing() {
        let m = Meta::open_in_memory().unwrap();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let part = "p0".to_string();
        let before = m.journal_len().unwrap();
        let t = f.ctime_ns + 10;
        let records = execute(
            &m,
            &MutateOp::AtimeBatch {
                entries: vec![(f.ino, t, t)],
            },
            None,
        )
        .unwrap();
        // The holder appends nothing to the write journal...
        assert!(records.is_empty());
        assert_eq!(m.journal_len().unwrap(), before);
        // ...applies to its own inode table...
        assert_eq!(m.getattr(f.ino).unwrap().unwrap().atime_ns, t);
        // ...and queues exactly one atime row for the shipper to drain.
        assert_eq!(m.atime_backlog_of(&part).unwrap(), 1);
    }

    #[test]
    fn mutate_op_roundtrip() {
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "a".into(),
            ino: 42,
            mode: 0o644,
            uid: 1,
            gid: 1,
        };
        let bytes = op.to_postcard().unwrap();
        assert_eq!(MutateOp::from_postcard(&bytes).unwrap(), op);
    }

    #[test]
    fn execute_create_journals_one_record() {
        let m = Meta::open_in_memory().unwrap();
        let records = execute(
            &m,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "f".into(),
                ino: (1 << 40) | 7,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            None,
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(
            &records[0],
            LogRecord::Create { name, ino, .. } if name == "f" && *ino == (1 << 40) | 7
        ));
        assert!(m.lookup(ROOT_INO, "f").unwrap().is_some());
    }

    #[test]
    fn execute_publish_carries_xattrs_and_roundtrips() {
        let m = Meta::open_in_memory().unwrap();
        let ino = (1 << 40) | 9;
        let op = MutateOp::Publish {
            ino,
            parent: ROOT_INO,
            name: "published".into(),
            mode: 0o640,
            uid: 1000,
            gid: 1000,
            mtime_ns: 123,
            manifest: b"MANIFEST".to_vec(),
            size: 42,
            xattrs: vec![("user.passsage.meta".into(), b"blob".to_vec())],
            noreplace: false,
        };
        let bytes = op.to_postcard().unwrap();
        assert_eq!(MutateOp::from_postcard(&bytes).unwrap(), op);

        let records = execute(&m, &op, None).unwrap();
        assert_eq!(records.len(), 3);
        assert!(matches!(records[0], LogRecord::Create { .. }));
        assert!(matches!(records[1], LogRecord::WriteManifest { .. }));
        assert!(matches!(
            &records[2],
            LogRecord::SetXattr { name, .. } if name == "user.passsage.meta"
        ));
        assert_eq!(
            m.get_xattr(ino, "user.passsage.meta").unwrap().as_deref(),
            Some(b"blob".as_slice())
        );
    }

    fn rid(seq: u64) -> crate::rid::Rid {
        crate::rid::Rid {
            node: 1,
            incarnation: 1,
            seq,
        }
    }

    /// Plan 30 §M2: passing a rid appends `LogRecord::Completed` in the
    /// same call, and it lands in the `completed` keyspace immediately
    /// (no separate replay step needed on the writer's own replica).
    #[test]
    fn execute_with_rid_appends_completed_and_populates_completed_keyspace() {
        let m = Meta::open_in_memory().unwrap();
        let r = rid(1);
        let records = execute(
            &m,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "f".into(),
                ino: (1 << 40) | 7,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(r),
        )
        .unwrap();
        assert_eq!(records.len(), 2, "op record + Completed");
        assert!(matches!(records[0], LogRecord::Create { .. }));
        assert!(matches!(records[1], LogRecord::Completed { rid } if rid == r));
        assert!(m.completed_position(r).unwrap().is_some());
    }

    /// A multi-record op (`Publish` appends 3) still gets exactly one
    /// `Completed`, appended once, in the same transaction — not once
    /// per underlying record.
    #[test]
    fn execute_with_rid_on_multi_record_op_completes_once() {
        let m = Meta::open_in_memory().unwrap();
        let r = rid(2);
        let ino = (1 << 40) | 9;
        let op = MutateOp::Publish {
            ino,
            parent: ROOT_INO,
            name: "published".into(),
            mode: 0o640,
            uid: 1000,
            gid: 1000,
            mtime_ns: 123,
            manifest: b"MANIFEST".to_vec(),
            size: 42,
            xattrs: vec![("user.passsage.meta".into(), b"blob".to_vec())],
            noreplace: false,
        };
        let records = execute(&m, &op, Some(r)).unwrap();
        assert_eq!(records.len(), 4, "3 op records + one Completed");
        let completed_count = records
            .iter()
            .filter(|r| matches!(r, LogRecord::Completed { .. }))
            .count();
        assert_eq!(completed_count, 1);
        assert!(m.completed_position(r).unwrap().is_some());
    }

    /// A refused op (here: `Create` on an already-existing name) is
    /// never completed — plan 30 §M2: "refusals are not recorded."
    #[test]
    fn execute_with_rid_on_refusal_does_not_complete() {
        let m = Meta::open_in_memory().unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "dup".into(),
            ino: (1 << 40) | 11,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        execute(&m, &op, Some(rid(3))).unwrap();
        let r2 = rid(4);
        let err = execute(&m, &op, Some(r2)).unwrap_err();
        assert!(matches!(err, MetaError::Exists));
        assert!(m.completed_position(r2).unwrap().is_none());
    }

    /// The review of the flaky completion test: `execute` used to return
    /// every journal row above the tip it read before the op ran — a
    /// window over the shared journal, not the op's own transaction. A
    /// row another writer committed inside that window (the holder's own
    /// client's op with its `Completed`, a snapshot row) rode the
    /// forwarded outcome into the requester's shadow under the
    /// requester's rid, was applied there out of log order (ahead of
    /// unshipped work it depends on), and widened the shadow's covering
    /// set to keys the requester's replica does not hold — a session read
    /// of such a key answered from the replica instead of waiting for
    /// the log. The outcome is now exactly the op's own rows.
    #[test]
    fn foreign_journal_rows_never_ride_a_forwarded_outcome() {
        use crate::session::{KeySet, Position, ReadKey};
        use crate::SnapshotRow;

        let holder = Meta::open_in_memory().unwrap();
        // The shipped prefix every replica has: `u`.
        holder.create(ROOT_INO, "u", 0o644, 0, 0).unwrap();
        let requester = replica_of(&holder);
        holder.ack_journal(u64::MAX).unwrap();
        // The holder's own client, unshipped: `w` (the requester lacks it).
        let w_ino = holder.allocate_ino(ROOT_INO).unwrap();
        let local = |seq| crate::rid::Rid {
            node: 1,
            incarnation: 1,
            seq,
        };
        execute(
            &holder,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "w".into(),
                ino: w_ino,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(local(1)),
        )
        .unwrap();

        // While the forwarded op runs, the holder's client renames `w` to
        // `v` and a snapshot row lands (before the op's transaction), and
        // the client unlinks `u` (after it) — each on its own thread, as
        // the FUSE fast path and the control service do.
        window_hook::set(Some(Box::new(move |m: &Meta, at| {
            std::thread::scope(|s| {
                s.spawn(|| match at {
                    window_hook::At::Before => {
                        execute(
                            m,
                            &rename_op(ROOT_INO, "w", ROOT_INO, "v", false),
                            Some(local(2)),
                        )
                        .unwrap();
                        m.record_snapshot(&SnapshotRow::new("s-foreign", "/", "s", "00", 0))
                            .unwrap();
                    }
                    window_hook::At::After => {
                        execute(
                            m,
                            &MutateOp::Unlink {
                                parent: ROOT_INO,
                                name: "u".into(),
                            },
                            Some(local(3)),
                        )
                        .unwrap();
                    }
                })
                .join()
                .unwrap();
            });
        })));
        let f_ino = holder.allocate_ino(ROOT_INO).unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "f".into(),
            ino: f_ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let rid_r = crate::rid::Rid {
            node: 2,
            incarnation: 1,
            seq: 1,
        };
        let records = execute(&holder, &op, Some(rid_r));
        window_hook::set(None);
        let records = records.unwrap();
        // The holder did journal all of it.
        assert_eq!(ino_at(&holder, ROOT_INO, "v"), Some(w_ino));
        assert_eq!(ino_at(&holder, ROOT_INO, "u"), None);

        // The requester installs the outcome as `rid_r`'s shadow and
        // covers its keys at the reply's position; one of its clients has
        // already observed that position (an earlier answer).
        assert!(requester.install_shadow(rid_r, 1, &op, &records).unwrap());
        let reply = Position {
            seq: 1,
            pending: None,
            streams: Default::default(),
        };
        requester.session().raise_observed(reply);
        requester
            .session()
            .note_covering(KeySet::from_records(&records), reply);
        let covered = |key: ReadKey| requester.session().ready(&[key], 0, false, &Position::ZERO);
        let seen = format!("outcome {records:?}");

        // Its own write: installed, and read without waiting.
        assert_eq!(ino_at(&requester, ROOT_INO, "f"), Some(f_ino), "{seen}");
        assert!(covered(ReadKey::Dentry(ROOT_INO, "f".into())), "{seen}");
        // Nobody else's row is installed under its rid: not the snapshot,
        // not the unlink of `u` that followed the op.
        assert!(
            requester.snapshots(None).unwrap().is_empty(),
            "a foreign snapshot row was installed as the requester's shadow: {seen}"
        );
        assert!(
            ino_at(&requester, ROOT_INO, "u").is_some(),
            "a foreign unlink was installed as the requester's shadow: {seen}"
        );
        // `v` is not on this replica (its create, `w`, is unshipped); a
        // read of it must wait for the log, not be answered `ENOENT` as
        // covered.
        assert_eq!(ino_at(&requester, ROOT_INO, "v"), None);
        assert!(
            !covered(ReadKey::Dentry(ROOT_INO, "v".into())),
            "a read of `v` was answered from a replica without it: {seen}"
        );
        // The holder's segment retires the shadow and the requester
        // converges on the log.
        let segment: Vec<LogRecord> = holder
            .peek_journal_after(0)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        requester
            .apply_segment(2, 1, &segment, &crate::replay::TouchSet::default())
            .unwrap();
        assert!(!requester.has_outstanding_speculation());
        assert_eq!(ino_at(&requester, ROOT_INO, "v"), Some(w_ino));
        assert_eq!(ino_at(&requester, ROOT_INO, "f"), Some(f_ino));
        assert_eq!(ino_at(&requester, ROOT_INO, "u"), None);
        assert_eq!(requester.snapshots(None).unwrap().len(), 1);
        // The outcome is the op's transaction and nothing else.
        assert!(
            matches!(
                records.as_slice(),
                [LogRecord::Create { name, ino, .. }, LogRecord::Completed { rid }]
                    if name == "f" && *ino == f_ino && *rid == rid_r
            ),
            "{seen}"
        );
    }
}
