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
        /// The file's mtime: the writer's last change, or a time set by
        /// `utimes` since. `None`: the commit's time.
        mtime_ns: Option<i64>,
        /// The writer's last data change (`WriteState::changed`, an HLC
        /// stamp; never a time a user set, unlike `mtime_ns`): with the
        /// row's ctime, what the commit leaves as the file's ctime
        /// (`store::writes::commit_ctime`). `None`: the commit's time (a
        /// scan, fsck, recovery: no writer's change to carry).
        ctime_ns: Option<i64>,
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
    /// Plan 30 §M14 phase 2 (the fencing token): refused because a grant
    /// the op was issued under is no longer live at the executor
    /// ([`MetaError::LockLapsed`]). Nothing was executed or journaled. A
    /// definitive outcome: the requester answers `EIO` and never retries
    /// or replays the op (a replay refused this way leaves a conflict
    /// copy, never the op's effect).
    LockLapsed,
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

/// Chunk metered-own-rows: what keeps a reply's position from the
/// requester's log, as the sequencer that answered it sees at reply time,
/// so that the requester waits only for what it does not already have.
///
/// The position names the sequencer's whole journal through it, and a
/// requester that observes it (a refusal, an op completed through the
/// log) waits in every later read until its applied log reaches it. A
/// `back` close of the requester's own answered at once keeps its chunks
/// held there on a metered network and its rows deferred on the
/// sequencer, below every later segment's `through`. Those rows are the
/// requester's own transactions, whose effects its replica already
/// carries (installed as shadows when they were answered): the requester
/// need not wait for them, nor upload to bring them ([`Self::settle`]).
/// Every other row through the position it still waits for — another
/// node's op interleaved below it ships on its own, unless it is deferred
/// behind the requester's chunks (its blame is in `others`: those are
/// uploaded).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OwnRows {
    /// The requester's own transactions still unshipped through the
    /// position, in journal order.
    pub txs: Vec<OwnTx>,
    /// The requester's inodes that every other held or deferred
    /// transaction through the position waits for (`Meta::remote_blockers`'
    /// blame).
    pub others: Vec<Ino>,
}

/// One of [`OwnRows::txs`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnTx {
    pub rid: crate::rid::Rid,
    /// Its journal rows on the sequencer, `first..=last`.
    pub first: u64,
    pub last: u64,
    /// Whether its rows change anything a read sees (a refusal's do not).
    pub effect: bool,
    /// The requester's inodes it waits for (empty: it ships on its own).
    pub waits: Vec<Ino>,
}

/// Most transactions of the requester's [`OwnRows`] names; with more, the
/// sequencer names none and the requester waits for everything, as
/// before.
pub const OWN_ROWS_CAP: usize = 64;

impl OwnRows {
    /// The requester's side. A transaction of its own whose effect its
    /// replica carries (`installed`: a shadow still outstanding, or the
    /// log delivered it), or that has none (a refusal), is *excused*: its
    /// rows are not waited for. Any other one (the op's own transaction
    /// that is still to come through the log, one of an earlier
    /// incarnation's) is waited for, and so is what it waits for.
    ///
    /// Returns the excused rows (journal seq ranges) and the inodes to
    /// upload: `others`, plus what every transaction not excused waits
    /// for.
    pub fn settle(
        &self,
        installed: impl Fn(crate::rid::Rid) -> bool,
    ) -> (Vec<(u64, u64)>, Vec<Ino>) {
        let mut excused = Vec::new();
        let mut upload: std::collections::BTreeSet<Ino> = self.others.iter().copied().collect();
        for t in &self.txs {
            if !t.effect || installed(t.rid) {
                excused.push((t.first, t.last));
            } else {
                upload.extend(&t.waits);
            }
        }
        (excused, upload.into_iter().collect())
    }

    /// The P2P wire form of an answer's `Option<OwnRows>`
    /// (`constellation_net::Payload::MutateReply::own_rows`): empty for
    /// `None`, else postcard.
    pub fn to_wire(rows: Option<&OwnRows>) -> Vec<u8> {
        rows.and_then(|r| postcard::to_allocvec(r).ok())
            .unwrap_or_default()
    }

    /// An empty or undecodable value reads as `None`: the requester then
    /// waits for everything, as before the signal.
    pub fn from_wire(bytes: &[u8]) -> Option<OwnRows> {
        if bytes.is_empty() {
            return None;
        }
        postcard::from_bytes(bytes).ok()
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
    execute_tagged(
        meta,
        op,
        rid,
        &crate::locks::LockTag::NONE,
        constellation_types::clock::Now::same(0),
    )
}

/// [`execute`] for an op issued under cluster-lock grants (plan 30 §M14
/// phase 2, the fencing token): refused with [`MetaError::LockLapsed`],
/// executing and journaling nothing, when a grant `tag` names is no
/// longer live at `now` on this executor's clock (its wall reading)
/// ([`crate::locks::LockTables::check_tag`]). Every execution funnel —
/// the holder's and a delegate's for a forwarded op, the lease path, a
/// replay, the inbox drain, the FUSE fast paths — comes through here, so
/// the check is where the write is recorded. The tag is recorded with the
/// op's `journal_tx` row for a replay after a deposition.
///
/// F1: `now` is read before the op waits for the store's writer lock
/// (a FUSE thread behind a slow writer, a core step that took its `now`
/// before it waited), so this check only refuses early. The one that
/// counts runs again inside the op's transaction, with the writer lock
/// held and this node's clock read there
/// ([`crate::locks::LockTables::check_tag_at_write`], from
/// `Meta::begin_local`), and pins the grants until the op is over: a
/// position this node reports as covering what it executed waits for it
/// (`Meta::settle_tagged_writes`).
pub fn execute_tagged(
    meta: &Meta,
    op: &MutateOp,
    rid: Option<crate::rid::Rid>,
    tag: &crate::locks::LockTag,
    now: constellation_types::clock::Now,
) -> Result<Vec<LogRecord>, MetaError> {
    if meta.locks().check_tag(tag, now).is_err() {
        return Err(MetaError::LockLapsed);
    }
    #[cfg(any(test, feature = "test-util"))]
    if !before_write::fire() {
        return Err(MetaError::NotAdmitted);
    }
    let _tag = crate::store::journal::PendingLockTag::set(tag, now, meta.locks());
    let _pending = crate::store::journal::PendingCompletion::set(rid);
    // Plan 30 §M3b: the op's own journaled transaction records `(rid, op)`
    // in its `journal_tx` row (`store::local`), so a deposition can replay
    // it by rid.
    let _op = crate::store::journal::PendingLocalOp::set(rid, op);
    crate::readdeleg::clear_victims();
    let records = execute_inner(meta, op)?;
    meta.note_unshipped(&records);
    note_landed(tag);
    Ok(records)
}

/// Evidence for a harness (off by default, the twin of `check_tag`'s
/// `tagged op executed`): when an op tagged with each grant *landed* — its
/// write transaction committed — on the host's clock, read now, after the
/// writer lock. Both tagged write funnels log it: [`execute_tagged`] and
/// `Meta::with_lock_tag` (the holder's own manifest commit). `check_tag` runs before the op waits for that lock, so its
/// line alone cannot show a write delayed past its grant (review finding
/// F1); the load campaign's token-order check takes the later of the two.
pub(crate) fn note_landed(tag: &crate::locks::LockTag) {
    if tag.is_empty()
        || !tracing::enabled!(target: "constellation::token_exec", tracing::Level::DEBUG)
    {
        return;
    }
    // Microseconds: a landing in the same millisecond as a successor's
    // grant line (which the tracing default stamps to the µs) is told
    // apart.
    let host_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    for t in &tag.0 {
        // In the message, not as fields, as `check_tag`'s line.
        tracing::debug!(
            target: "constellation::token_exec",
            "tagged op landed minter={} seq={} host_us={host_us}",
            t.grant.node,
            t.grant.seq,
        );
    }
}

/// [`execute_tagged`] on a FUSE fast path: `still_admitted` says whether
/// what admitted the op there (the lease, a delegation) still does, and is
/// asked again once the op's transaction holds the store's writer lock —
/// `false` then refuses the op with [`MetaError::NotAdmitted`], nothing
/// journaled, and the caller sends it through the authority core (F1,
/// F9: an op that waited for the lock past its lease's expiry journaled a
/// row a takeover strands and replays).
pub fn execute_admitted(
    meta: &Meta,
    op: &MutateOp,
    rid: Option<crate::rid::Rid>,
    tag: &crate::locks::LockTag,
    now: constellation_types::clock::Now,
    still_admitted: Box<dyn Fn() -> bool>,
) -> Result<Vec<LogRecord>, MetaError> {
    let _admission = crate::store::journal::PendingAdmission::set(still_admitted);
    execute_tagged(meta, op, rid, tag, now)
}

/// Tests only (`test-util`; the simulation's `WriterLockHold`, F1): a hook this thread
/// runs inside [`execute_tagged`] after the check before the op and before
/// its transaction — where a FUSE thread waits for the store's writer lock
/// behind a slow writer. `false` abandons the op unwritten
/// ([`MetaError::NotAdmitted`]: its node died meanwhile). One
/// thread-local look per op while unset.
#[cfg(any(test, feature = "test-util"))]
pub fn set_before_write_hook_for_tests(hook: Option<Box<dyn FnMut() -> bool>>) {
    before_write::HOOK.with(|h| *h.borrow_mut() = hook);
}

#[cfg(any(test, feature = "test-util"))]
mod before_write {
    use std::cell::RefCell;

    type Hook = Box<dyn FnMut() -> bool>;

    thread_local! {
        pub(super) static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(super) fn fire() -> bool {
        let hook = HOOK.with(|h| h.borrow_mut().take());
        let Some(mut hook) = hook else {
            return true;
        };
        let go = hook();
        HOOK.with(|h| *h.borrow_mut() = Some(hook));
        go
    }
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
            mtime_ns,
            ctime_ns,
        } => {
            meta.set_manifest_with_base(
                *ino,
                base_manifest.as_deref(),
                manifest,
                *size,
                *mtime_ns,
                *ctime_ns,
            )?;
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
    use constellation_types::clock::Now;

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

    /// A rename changes the moved inode's ctime (Linux file systems do;
    /// it is a status change), on the holder and on a replica replaying
    /// the record, and the dentry copy of the attributes follows.
    #[test]
    fn rename_touches_the_moved_inodes_ctime() {
        let m = Meta::open_in_memory().unwrap();
        let a = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        execute(&m, &rename_op(ROOT_INO, "a", ROOT_INO, "b", false), None).unwrap();
        let r = replica_of(&m);
        for meta in [&m, &r] {
            let attr = meta.getattr(a.ino).unwrap().unwrap();
            assert!(
                attr.ctime_ns > a.ctime_ns,
                "{} vs {}",
                attr.ctime_ns,
                a.ctime_ns
            );
            let listed = meta.lookup(ROOT_INO, "b").unwrap().unwrap();
            assert_eq!(listed.ctime_ns, attr.ctime_ns);
        }
    }

    /// A manifest commit's ctime is the file's last change, not the
    /// commit's time (xcheckfs: `write; fchmod; close` moved ctime to the
    /// close): the write session's last data change, which the op carries,
    /// or any change since, already on the row — never the mtime, which a
    /// `futimens` may have set anywhere (`tar x`, `cp -p`, `touch -d`). A
    /// replica replays the same times.
    #[test]
    fn a_manifest_commit_keeps_the_ctime_of_the_files_last_change() {
        let m = Meta::open_in_memory().unwrap();
        let commit = |ino: Ino, mtime_ns: Option<i64>, ctime_ns: Option<i64>| {
            execute(
                &m,
                &MutateOp::SetManifest {
                    ino,
                    base_manifest: None,
                    manifest: b"MANIFEST".to_vec(),
                    size: 4,
                    mtime_ns,
                    ctime_ns,
                },
                None,
            )
            .unwrap();
            m.getattr(ino).unwrap().unwrap()
        };
        let create = |name: &str| m.create(ROOT_INO, name, 0o644, 0, 0).unwrap();
        let setattr = |ino: Ino, mode: Option<u32>, mtime_ns: Option<i64>| {
            let op = MutateOp::Setattr {
                ino,
                mode,
                uid: None,
                gid: None,
                size: None,
                atime_ns: None,
                mtime_ns,
            };
            execute(&m, &op, None).unwrap();
            m.getattr(ino).unwrap().unwrap().ctime_ns
        };

        // A write, then an fchmod, then the close: the fchmod's ctime.
        let a = create("a");
        let wrote = crate::hlc::now_ns();
        let chmodded = setattr(a.ino, Some(0o755), None);
        assert!(chmodded > wrote);
        let a = commit(a.ino, Some(wrote), Some(wrote));
        assert_eq!((a.ctime_ns, a.mtime_ns), (chmodded, wrote));

        // A write alone: the write's time, for both.
        let b = create("b");
        let wrote = crate::hlc::now_ns();
        let b = commit(b.ino, Some(wrote), Some(wrote));
        assert_eq!((b.ctime_ns, b.mtime_ns), (wrote, wrote));

        // A write, then a `futimens` an hour ahead, then the close: the
        // `futimens`'s ctime; the mtime set is no change time.
        let c = create("c");
        let wrote = crate::hlc::now_ns();
        let future = wrote + 3_600_000_000_000;
        let set = setattr(c.ino, None, Some(future));
        let c = commit(c.ino, Some(future), Some(wrote));
        assert_eq!((c.ctime_ns, c.mtime_ns), (set, future));

        // No change of the writer's (a scan, fsck, recovery): the
        // commit's time, for both.
        let d = create("d");
        let d0 = d.ctime_ns;
        let d = commit(d.ino, None, None);
        assert!(d.ctime_ns > d0);
        assert_eq!(d.mtime_ns, d.ctime_ns);

        let r = replica_of(&m);
        for attr in [a, b, c, d] {
            let replayed = r.getattr(attr.ino).unwrap().unwrap();
            assert_eq!(
                (replayed.ctime_ns, replayed.mtime_ns),
                (attr.ctime_ns, attr.mtime_ns)
            );
        }
    }

    /// Clock skew: a close forwarded from a node whose clock is ahead of
    /// the sequencer's keeps the ctime that node showed before its close
    /// (a ctime never goes back), and the sequencer's next change of the
    /// file stamps above it. 20 ms: the clock is process-wide.
    #[test]
    fn a_forwarded_close_from_a_node_ahead_keeps_its_ctime() {
        let m = Meta::open_in_memory().unwrap();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let ahead = crate::hlc::now_ns() + 20_000_000;
        let op = MutateOp::SetManifest {
            ino: f.ino,
            base_manifest: None,
            manifest: b"MANIFEST".to_vec(),
            size: 4,
            mtime_ns: Some(ahead),
            ctime_ns: Some(ahead),
        };
        execute(&m, &op, None).unwrap();
        let closed = m.getattr(f.ino).unwrap().unwrap();
        assert_eq!((closed.ctime_ns, closed.mtime_ns), (ahead, ahead));
        let chmod = MutateOp::Setattr {
            ino: f.ino,
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
        };
        execute(&m, &chmod, None).unwrap();
        assert!(m.getattr(f.ino).unwrap().unwrap().ctime_ns > ahead);
    }

    /// A forwarding client whose clock is far ahead (a bad RTC) moves no
    /// clock: its stamp is past the skew tolerance, so the sequencer does
    /// not observe it, and the cap cuts the file's ctime to the commit's
    /// time — nothing a day ahead reaches the record, or any replica's
    /// clock from it. The mtime it carries applies as is (any `futimens`
    /// may set one a day ahead).
    // Relies on no other test in this binary observing a far-ahead stamp
    // (the clock is process-wide).
    #[test]
    fn a_forwarded_close_from_a_clock_far_ahead_moves_no_clock() {
        let m = Meta::open_in_memory().unwrap();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let day_ahead = crate::hlc::now_ns() + 86_400_000_000_000;
        let op = MutateOp::SetManifest {
            ino: f.ino,
            base_manifest: None,
            manifest: b"MANIFEST".to_vec(),
            size: 4,
            mtime_ns: Some(day_ahead),
            ctime_ns: Some(day_ahead),
        };
        execute(&m, &op, None).unwrap();
        assert!(crate::hlc::last_ns() < day_ahead, "the clock was dragged");
        let closed = m.getattr(f.ino).unwrap().unwrap();
        assert!(closed.ctime_ns > f.ctime_ns && closed.ctime_ns < day_ahead);
        assert_eq!(closed.mtime_ns, day_ahead);
        let records = m.peek_journal_after(0).unwrap();
        let stamps: Vec<i64> = records.iter().filter_map(|(_, r)| r.stamp_ns()).collect();
        assert!(stamps.iter().all(|t| *t < day_ahead), "{stamps:?}");
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

    /// F1 (`cst-load-review` §2): the fencing token is checked at the
    /// write, not only before the op waits for the store's writer lock.
    /// Node 2's rename under its exclusive grant passes the check at
    /// `t0`, then waits for the lock (a slow writer ahead of it, the
    /// finding-08 shape); meanwhile the minter outwaits the grant (and
    /// may grant node 3, whose ops then go ahead with priority). The
    /// stale rename must be refused (`LockLapsed` → `EIO`), journaling
    /// nothing; it used to land and be answered `OK`, overwriting the
    /// next holder's ref update.
    #[test]
    fn a_grant_ended_while_its_write_waited_is_refused_at_the_write() {
        use crate::locks::{LockMode, LockTag, LockToken};
        let m = Meta::open_in_memory().unwrap();
        let tmp = m.create(ROOT_INO, "tmp", 0o644, 0, 0).unwrap();
        let lock_file = m.create(ROOT_INO, "turn.lock", 0o644, 0, 0).unwrap();
        let g = m.locks().grant(
            1,
            2,
            lock_file.ino,
            LockMode::Exclusive,
            constellation_types::clock::WaitMs(30_000),
            0,
        );
        let tag = LockTag(vec![LockToken {
            grant: g,
            until_ms: 28_000,
        }]);
        let before = m.journal_len().unwrap();
        // The grant ends at the minter while the op waits for the writer
        // lock (`At::Before`: right before the op's transaction).
        window_hook::set(Some(Box::new(move |m: &Meta, at| {
            if at == window_hook::At::Before {
                m.locks().forget(g);
            }
        })));
        let r = execute_tagged(
            &m,
            &rename_op(ROOT_INO, "tmp", ROOT_INO, "ref", false),
            None,
            &tag,
            Now::same(1_000),
        );
        window_hook::set(None);
        assert!(
            matches!(r, Err(MetaError::LockLapsed)),
            "a write under a grant that ended while it waited for the writer lock landed: {r:?}"
        );
        assert_eq!(ino_at(&m, ROOT_INO, "tmp"), Some(tmp.ino));
        assert_eq!(ino_at(&m, ROOT_INO, "ref"), None);
        assert_eq!(m.journal_len().unwrap(), before, "nothing journaled");
        assert!(m.locks().stats().token_rejections >= 1);
    }

    /// F1: an executor that did not mint the grant judges the token by
    /// its window, on its own clock *at the write*. The check before the
    /// op read a clock from before it waited for the writer lock (a core
    /// step's `now`); the window ended during the wait.
    #[test]
    fn a_window_that_ended_while_its_write_waited_is_refused_at_the_write() {
        use crate::locks::{GrantId, LockTag, LockToken};
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::sync::Arc;
        let m = Meta::open_in_memory().unwrap();
        let clock = Arc::new(AtomicI64::new(1_000));
        let c = clock.clone();
        m.locks()
            .set_clock(Arc::new(move || Now::same(c.load(Ordering::SeqCst))));
        m.create(ROOT_INO, "tmp", 0o644, 0, 0).unwrap();
        // Another sequencer's grant: judged by its window here.
        let tag = LockTag(vec![LockToken {
            grant: GrantId { node: 9, seq: 1 },
            until_ms: 5_000,
        }]);
        let c = clock.clone();
        window_hook::set(Some(Box::new(move |_: &Meta, at| {
            if at == window_hook::At::Before {
                c.store(5_000, Ordering::SeqCst);
            }
        })));
        let r = execute_tagged(
            &m,
            &rename_op(ROOT_INO, "tmp", ROOT_INO, "ref", false),
            None,
            &tag,
            Now::same(1_000),
        );
        window_hook::set(None);
        assert!(matches!(r, Err(MetaError::LockLapsed)), "{r:?}");
        assert_eq!(ino_at(&m, ROOT_INO, "ref"), None);
        assert_eq!(m.locks().stats().token_rejections_at_write, 1);
        // Inside its window the same op goes through, and leaves no pin.
        clock.store(1_000, Ordering::SeqCst);
        execute_tagged(
            &m,
            &rename_op(ROOT_INO, "tmp", ROOT_INO, "ref", false),
            None,
            &tag,
            Now::same(1_000),
        )
        .unwrap();
        assert!(!m.locks().unsettled_writes());
    }

    /// F1: a tagged write that passed its check at the write is *pinned*
    /// until its transaction is over. Once its grant has ended here, a
    /// position this node reports (a grant's floor, a cut) first waits
    /// for that write (`Meta::settle_tagged_writes`), so a next holder's
    /// floor never lacks a write that lands after its grant — the commit
    /// of a write checked in time can still take seconds (a slow disk).
    #[test]
    fn a_position_taken_after_a_grant_ends_waits_for_its_pinned_write() {
        use crate::locks::{LockMode, LockTag, LockToken};
        use crate::store::journal::write_check_hook;
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::time::Duration;
        let m = Arc::new(Meta::open_in_memory().unwrap());
        m.create(ROOT_INO, "tmp", 0o644, 0, 0).unwrap();
        let lock_file = m.create(ROOT_INO, "turn.lock", 0o644, 0, 0).unwrap();
        let g = m.locks().grant(
            1,
            1,
            lock_file.ino,
            LockMode::Exclusive,
            constellation_types::clock::WaitMs(30_000),
            0,
        );
        let tag = LockTag(vec![LockToken {
            grant: g,
            until_ms: 28_000,
        }]);
        // Live grant, nothing in a transaction: nothing to settle.
        assert!(!m.locks().unsettled_writes());
        assert!(!m.settle_tagged_writes());
        let (pinned_tx, pinned_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let writer = {
            let m = m.clone();
            let tag = tag.clone();
            std::thread::spawn(move || {
                write_check_hook::set(Some(Box::new(move |at| {
                    if at == write_check_hook::At::Pinned {
                        pinned_tx.send(()).unwrap();
                        // The commit is slow: the writer lock stays held.
                        go_rx.recv().unwrap();
                    }
                    true
                })));
                let r = execute_tagged(
                    &m,
                    &rename_op(ROOT_INO, "tmp", ROOT_INO, "ref", false),
                    None,
                    &tag,
                    Now::same(1_000),
                );
                write_check_hook::set(None);
                r
            })
        };
        pinned_rx.recv().unwrap();
        // Pinned under a live grant: a position need not wait for it (no
        // conflicting grant can be made while the grant lives here).
        assert!(!m.locks().unsettled_writes());
        // The grant ends at the minter (an outwait) while the write is
        // in its transaction.
        m.locks().forget(g);
        assert!(m.locks().unsettled_writes());
        let (done_tx, done_rx) = mpsc::channel();
        let settler = {
            let m = m.clone();
            std::thread::spawn(move || {
                let waited = m.settle_tagged_writes();
                // What a floor read now covers: the journal as it is.
                let renamed = ino_at(&m, ROOT_INO, "ref").is_some();
                done_tx.send(()).unwrap();
                (waited, renamed)
            })
        };
        assert!(
            done_rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "a position was taken while a write under the ended grant was still committing"
        );
        go_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        let (waited, renamed) = settler.join().unwrap();
        assert!(waited);
        assert!(renamed, "the settled position lacks the pinned write");
        assert!(!m.locks().unsettled_writes(), "the pin is gone with the op");
        assert_eq!(m.locks().stats().settle_waits, 1);
        // A write that comes after the end is refused at once.
        let r = execute_tagged(
            &m,
            &rename_op(ROOT_INO, "ref", ROOT_INO, "ref2", false),
            None,
            &tag,
            Now::same(1_000),
        );
        assert!(matches!(r, Err(MetaError::LockLapsed)), "{r:?}");
    }

    /// F1/F9: a fast path's admission is judged again at the write; one
    /// that ran out while the op waited for the writer lock refuses it
    /// unwritten (`NotAdmitted`: the caller sends it through the core).
    #[test]
    fn an_admission_that_ran_out_before_the_write_refuses_it_unwritten() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let m = Meta::open_in_memory().unwrap();
        m.create(ROOT_INO, "tmp", 0o644, 0, 0).unwrap();
        let before = m.journal_len().unwrap();
        let usable = Arc::new(AtomicBool::new(true));
        let u = usable.clone();
        window_hook::set(Some(Box::new(move |_: &Meta, at| {
            if at == window_hook::At::Before {
                u.store(false, Ordering::SeqCst);
            }
        })));
        let u = usable.clone();
        let r = execute_admitted(
            &m,
            &rename_op(ROOT_INO, "tmp", ROOT_INO, "ref", false),
            None,
            &crate::locks::LockTag::NONE,
            Now::same(0),
            Box::new(move || u.load(Ordering::SeqCst)),
        );
        window_hook::set(None);
        assert!(matches!(r, Err(MetaError::NotAdmitted)), "{r:?}");
        assert_eq!(ino_at(&m, ROOT_INO, "ref"), None);
        assert_eq!(m.journal_len().unwrap(), before);
        // Admitted throughout: executed. The admission is gone after the
        // call (a later plain execution is not judged by it).
        usable.store(true, Ordering::SeqCst);
        let u = usable.clone();
        execute_admitted(
            &m,
            &rename_op(ROOT_INO, "tmp", ROOT_INO, "ref", false),
            None,
            &crate::locks::LockTag::NONE,
            Now::same(0),
            Box::new(move || u.load(Ordering::SeqCst)),
        )
        .unwrap();
        usable.store(false, Ordering::SeqCst);
        execute(
            &m,
            &rename_op(ROOT_INO, "ref", ROOT_INO, "ref2", false),
            None,
        )
        .unwrap();
    }

    /// F1 (review M2): a write pinned under another sequencer's grant (a
    /// delegate running a write under a lock the root minted) needs no
    /// settle while its token's window is open here: the next grant
    /// follows only an outwait, after that window. Past it, it does. One
    /// under a grant this table ended needs it at once.
    #[test]
    fn a_pin_under_a_foreign_grant_is_unsettled_only_past_its_window() {
        use crate::locks::{GrantId, LockMode, LockTag, LockToken};
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::sync::Arc;
        let m = Meta::open_in_memory().unwrap();
        let clock = Arc::new(AtomicI64::new(1_000));
        let c = clock.clone();
        m.locks()
            .set_clock(Arc::new(move || Now::same(c.load(Ordering::SeqCst))));
        let foreign = LockTag(vec![LockToken {
            grant: GrantId { node: 9, seq: 1 },
            until_ms: 5_000,
        }]);
        m.locks()
            .check_tag_at_write(&foreign, Now::same(1_000))
            .unwrap();
        assert!(!m.locks().unsettled_writes(), "inside its window");
        clock.store(5_000, Ordering::SeqCst);
        assert!(m.locks().unsettled_writes(), "past its window");
        m.locks().unpin(&foreign);
        assert!(!m.locks().unsettled_writes());
        // Minted here: settles once ended, whatever the token's window.
        let f = m.create(ROOT_INO, "turn.lock", 0o644, 0, 0).unwrap();
        let g = m.locks().grant(
            1,
            2,
            f.ino,
            LockMode::Exclusive,
            constellation_types::clock::WaitMs(60_000),
            0,
        );
        let own = LockTag(vec![LockToken {
            grant: g,
            until_ms: 50_000,
        }]);
        m.locks()
            .check_tag_at_write(&own, Now::same(5_000))
            .unwrap();
        assert!(!m.locks().unsettled_writes(), "live here");
        m.locks().forget(g);
        assert!(m.locks().unsettled_writes(), "ended here");
        m.locks().unpin(&own);
    }
}
