//! Plan 32 §6.2: the per-snapshot space-accounting index.
//!
//! `snapshot ls` shows ZFS's numbers for every snapshot — `USED` (bytes
//! only this snapshot keeps), `WRITTEN` (bytes new since the chain's
//! previous surviving snapshot), `REFER` and `LSIZE` — and `snapshot
//! delete --dry-run` answers `reclaim(D)`, "what would deleting this set
//! return". Computing those on demand is a walk of every snapshot
//! (ONTAP's `compute-reclaimable`, capped at three snapshots for that
//! reason); computing them on the write path is btrfs qgroups, which
//! distros turn off (L10). This index is the third way: derived from the
//! snapshot rows, node-local, never published, maintained off-path from
//! [`crate::snapwalk`]'s occurrence deltas, and every operation costs
//! O(what changed). It is advisory — GC never consults it — and it can be
//! deleted at any time to force a rebuild.
//!
//! ## Chains, ordinals, runs
//!
//! The snapshots of one directory form a **chain** (a small `u32` id,
//! registered against the directory's ino). Each snapshot gets an
//! **ordinal** from the chain's counter, increasing in chain order;
//! deletion leaves gaps, so `prev(k)`/`next(k)` are the neighbouring
//! *existing* ordinals.
//!
//! A chunk's presence in a chain is stored as **runs**, `(chain, first,
//! last | OPEN, occ, size)`, one set per derived size (see "Sizes"). The
//! invariant, per size:
//!
//! > Within a chain, a chunk is present at a size in exactly the snapshots
//! > whose ordinals fall in one of its runs of that size. Runs are maximal
//! > (between two runs of one chain and size there is an existing
//! > snapshot without the chunk at that size), their endpoints are
//! > existing snapshots, and a run is **open** exactly when the chunk is
//! > present at its size in the chain's newest snapshot (the head). `occ`
//! > is the chunk's occurrence count at that size at the head, for open
//! > runs only.
//!
//! ## Sizes
//!
//! A chunk's size is derived per occurrence (`min(chunk_size, file_len −
//! offset)`), and one hash can occur at two sizes: a tail chunk whose file
//! was extended past it, or cut inside it, keeps its hash. The chunk
//! counts at the **largest** size any snapshot holds it at (plan 32
//! §6.1), so every number is a function of the snapshots alone, whatever
//! order the index applied them in. Hence runs per size: the chunk is in a
//! snapshot when any of its runs covers it (`REFER`, `WRITTEN`,
//! ownership and `reclaim` look at that union), and its size is its runs'
//! largest — which can only change when a run of a new largest size opens
//! or the last run of the largest goes. The operations below work at the
//! entry's size so far; `ops::Ctx::flush` then moves every snapshot
//! holding the chunk to the new size.
//!
//! Applying a new snapshot's deltas is then local: a count crossing zero
//! upwards opens a run at the new ordinal, one crossing downwards closes
//! the open run at the previous head, and a chunk whose count does not
//! change needs no work at all, because an open run extends implicitly.
//! Every run is indexed by its birth (`snapacct_birth`, `chain|first|hash`)
//! and, once closed, its death (`snapacct_death`, `chain|last|hash`), so
//! deleting a snapshot visits only the runs that begin or end at it, and
//! `reclaim(D)` only the runs born inside D's ordinal ranges.
//!
//! ## Why not deadlists
//!
//! ZFS keeps, per snapshot, the blocks that died at it, keyed by birth
//! time. That needs **no afterlife** — a block, once freed, never comes
//! back — and one lineage. Content addressing breaks both (L11): a
//! reverted file references the old chunk again, and dedup shares one
//! chunk across unrelated chains. Runs express both directly: an afterlife
//! is a second run in the same chain, cross-chain sharing is runs in two
//! chains. Because a run carries its death ordinal, a range query does not
//! need ZFS's O(n²) sublist walk either.
//!
//! ## Ownership and the buckets
//!
//! A chunk's **sole owner** is the one snapshot that references it when
//! nothing else does: not live, exactly one run, and that run covers one
//! snapshot (`first == last`, or open and born at the head). After any
//! change to a chunk entry its owner is recomputed and its size moves
//! between the owners' `USED`; the same transition moves it between the
//! filesystem buckets (`unique`, `shared` by ≥2 snapshots only, shared
//! with `live`). That is why deleting S makes the chunks S shared with
//! exactly one neighbour appear in that neighbour's `USED` — ZFS's
//! documented behaviour. The head matters for open runs, so whenever a
//! chain's head moves the runs born at the old and the new head are
//! re-examined (O(`WRITTEN` of that snapshot)).
//!
//! A chunk that loses its last run while not live becomes a **tombstone**
//! (`{size, since_ms}`): snapshots freed it and GC will delete it after
//! its horizon. Tombstones older than `gc.horizon + gc interval` are
//! presumed collected and dropped ([`SnapAcct::expire_tombstones`]); one
//! referenced again (a run, or the live tree) is revived. Σ tombstones is
//! `snapshot space`'s "awaiting GC" line.
//!
//! ## What callers supply
//!
//! The index never reads the tree. Its inputs are the snapwalk deltas
//! ([`crate::snapwalk::Delta`]), an `lsize` delta per snapshot, and a
//! liveness oracle consulted only for a chunk that enters the index.
//! Occurrence counts are known only at a chain's head, so the two
//! operations that move the head *backwards* need the deltas that lead
//! back to the new head: deleting the head takes `step(prev, head)`, and
//! inserting a snapshot that sorts before the head
//! ([`SnapAcct::rederive_suffix`]) rewinds the chain to the insertion
//! point with `step(P, head)` and re-applies the suffix from the caller's
//! steps. That costs one `ChainWalk::step` per snapshot after the
//! insertion point plus one for the rewind; it happens only when a row is
//! applied late, so it is rare. Ordinals are never reused or reordered:
//! the re-applied suffix takes fresh ones from the chain counter.
//!
//! ## Replay
//!
//! The header's `accounted_seq` is the replicated commit sequence the
//! index is "as of". Every mutation takes `seq: Option<u64>` and, when it
//! is given, writes it in the same transaction as the change, so after a
//! crash the index holds either both or neither. The contract for the
//! caller: apply each commit's snapshot changes with that commit's seq,
//! skip any commit whose seq is `<= accounted_seq()` on restart, and
//! advance over commits that change no snapshot with
//! [`SnapAcct::set_accounted_seq`]. The seq never moves backwards (that is
//! refused). A commit that needs several calls is atomic only per call:
//! pass its seq on the last one and make the earlier ones tolerate a
//! replay ([`SnapAcct::locate`] says whether a snapshot is already
//! indexed).

mod encoding;
mod ops;
pub mod service;
mod verify;

#[cfg(test)]
mod service_tests;
#[cfg(test)]
mod tests;

pub use encoding::ChunkEntry;
pub use ops::NewSnapshot;
pub use service::{
    BudgetAnswer, BudgetPlan, Numbers, ReclaimEstimate, SnapAcctConfig, SnapAcctDeps, SnapAcctMode,
    SnapAcctService, SnapAcctStats, SnapAnswer, SpaceBreakdown, VerifyReport,
};

use crate::snapwalk::{Delta, Occurrences};
use constellation_fs_core::ChunkHash;
use encoding::*;
use fjall::{KeyspaceCreateOptions, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The `last` of an open run.
pub const OPEN: u32 = u32::MAX;

/// The empty file that marks a directory as an index [`SnapAcct::open`]
/// created, and so may wipe.
pub const MARKER: &str = "SNAPACCT";

/// The fjall database, inside the index directory.
const DB_DIR: &str = "db";

/// One maximal interval of a chunk's presence in a chain at one derived
/// size. A chunk occurs at one size almost always; one that occurs at
/// two (a tail chunk whose file was extended past it) has a run per size,
/// and runs of different sizes may overlap. The chunk is in a snapshot
/// when any of its runs covers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Run {
    pub chain: u32,
    pub first: u32,
    /// The last snapshot containing the chunk at this size, or [`OPEN`]
    /// while the chain's head still does.
    pub last: u32,
    /// Occurrences at this size at the head; 0 for a closed run.
    pub occ: u64,
    /// The derived plaintext size of these occurrences.
    pub size: u64,
}

impl Run {
    pub fn is_open(&self) -> bool {
        self.last == OPEN
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapAcctError {
    #[error("snapshot accounting store: {0}")]
    Store(#[from] fjall::Error),
    #[error("snapshot accounting directory: {0}")]
    Io(#[from] std::io::Error),
    /// The index contradicts itself. It is derived state: drop it (or the
    /// chain, [`SnapAcct::clear_chain`]) and rebuild.
    #[error("snapshot accounting index is inconsistent: {0}")]
    Corrupt(String),
    /// The caller's input contradicts the index (deltas against the wrong
    /// base, an unknown chain or snapshot, a duplicate id).
    #[error("snapshot accounting input rejected: {0}")]
    Invalid(String),
}

pub type Result<T, E = SnapAcctError> = std::result::Result<T, E>;

/// How long a tombstone lives: GC deletes an unreferenced chunk once it
/// is older than its horizon, and runs every interval, so after the sum
/// the chunk is presumed gone.
#[derive(Clone, Copy, Debug)]
pub struct SnapAcctParams {
    pub gc_horizon_ms: u64,
    pub gc_interval_ms: u64,
    /// fjall block cache for this database.
    pub cache_bytes: u64,
}

impl Default for SnapAcctParams {
    fn default() -> Self {
        SnapAcctParams {
            gc_horizon_ms: 7 * 24 * 3600 * 1000,
            gc_interval_ms: 3600 * 1000,
            cache_bytes: 16 << 20,
        }
    }
}

/// What [`SnapAcct::open`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Opened {
    /// No index there yet: build it.
    Fresh,
    /// A matching index: resume from its `accounted_seq`.
    Reused,
    /// An index of another format or filesystem was wiped: build it.
    Rebuilt,
}

/// Bytes and chunk count of a set of chunks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Amount {
    pub bytes: u64,
    pub chunks: u64,
}

impl From<(u64, u64)> for Amount {
    fn from((bytes, chunks): (u64, u64)) -> Self {
        Amount { bytes, chunks }
    }
}

/// One snapshot's numbers (plan 32 §6.1), logical bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapNumbers {
    pub chain: u32,
    pub ord: u32,
    pub id: String,
    pub root: String,
    pub used: u64,
    pub written: u64,
    pub refer: u64,
    pub lsize: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainNumbers {
    pub chain: u32,
    pub dir_ino: u64,
    pub snapshots: u32,
    pub first: Option<u32>,
    pub head: Option<u32>,
    /// Σ `USED` (not what deleting the chain returns: see
    /// [`SnapAcct::reclaim`]).
    pub used: u64,
    /// Σ `WRITTEN`: every byte any run of this chain was born with.
    pub written: u64,
}

/// `snapshot space`'s breakdown (plan 32 Step 5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FsBreakdown {
    /// `usedbysnapshots` = `reclaim(all snapshots)`: indexed, not live.
    pub snapshots_total: Amount,
    /// Σ `USED`.
    pub unique: Amount,
    /// Not live, in ≥2 snapshots.
    pub shared_only: Amount,
    /// In some snapshot and in the live tree (costs nothing extra).
    pub shared_with_live: Amount,
    /// Freed by snapshot deletion, presumed not yet collected.
    pub awaiting_gc: Amount,
}

type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The index: one fjall database in its own directory.
pub struct SnapAcct {
    db: SingleWriterTxDatabase,
    chunk: SingleWriterTxKeyspace,
    birth: SingleWriterTxKeyspace,
    death: SingleWriterTxKeyspace,
    snap: SingleWriterTxKeyspace,
    meta: SingleWriterTxKeyspace,
    tomb: SingleWriterTxKeyspace,
    /// The live tree's spilled chunk lists and their members
    /// ([`SnapAcct::record_live_spill`]): `chunk_ref` rows name a spilled
    /// list, never its members, so this is how a member's liveness is
    /// known.
    lspill: SingleWriterTxKeyspace,
    params: SnapAcctParams,
    clock: Clock,
    path: PathBuf,
}

impl SnapAcct {
    /// Open (or create) the index in `dir`, which it owns: on a header
    /// mismatch (another format, another filesystem) or a header that
    /// does not decode, the index is wiped and starts empty.
    ///
    /// `dir` must be missing, empty, or an index this function created
    /// (it holds a [`MARKER`] file); anything else is refused with
    /// [`SnapAcctError::Invalid`] rather than deleted, so a wrong path
    /// can never cost the caller a directory.
    pub fn open(dir: &Path, fs_uuid: &str, params: SnapAcctParams) -> Result<(SnapAcct, Opened)> {
        let marker = dir.join(MARKER);
        if !marker.exists() {
            if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
                return Err(SnapAcctError::Invalid(format!(
                    "{} is not empty and is not a snapshot accounting index; refusing to use it",
                    dir.display()
                )));
            }
            std::fs::create_dir_all(dir)?;
            std::fs::write(&marker, b"")?;
        }
        let ix = Self::open_raw(dir, params)?;
        let outcome = match ix.meta.get(META_HEADER)? {
            Some(v) => match from_postcard::<Header>(&v, "header") {
                Ok(h) if h.format == FORMAT && h.fs_uuid == fs_uuid => {
                    return Ok((ix, Opened::Reused))
                }
                // A damaged header, or a later format of another shape.
                Ok(_) | Err(_) => Opened::Rebuilt,
            },
            None => Opened::Fresh,
        };
        // A header is written with the database's first transaction, so
        // a database without a matching one is stale (or a crash before
        // the first commit): start over. The marker says the directory is
        // ours to wipe.
        drop(ix);
        let db = dir.join(DB_DIR);
        if db.exists() {
            std::fs::remove_dir_all(&db)?;
        }
        let ix = Self::open_raw(dir, params)?;
        let mut tx = ix.db.write_tx();
        tx.insert(
            &ix.meta,
            META_HEADER,
            to_postcard(&Header {
                format: FORMAT,
                fs_uuid: fs_uuid.to_string(),
                accounted_seq: 0,
            }),
        );
        tx.commit()?;
        Ok((ix, outcome))
    }

    fn open_raw(dir: &Path, params: SnapAcctParams) -> Result<SnapAcct> {
        let db = SingleWriterTxDatabase::builder(dir.join(DB_DIR))
            .cache_size(params.cache_bytes)
            .worker_threads(1)
            .open()?;
        let ks = |name: &str| db.keyspace(name, KeyspaceCreateOptions::default);
        Ok(SnapAcct {
            chunk: ks("snapacct_chunk")?,
            birth: ks("snapacct_birth")?,
            death: ks("snapacct_death")?,
            snap: ks("snapacct_snap")?,
            meta: ks("snapacct_meta")?,
            tomb: ks("snapacct_tomb")?,
            lspill: ks("snapacct_lspill")?,
            db,
            params,
            clock: Arc::new(system_ms),
            path: dir.to_path_buf(),
        })
    }

    /// Replace the wall clock tombstones are stamped and expired with.
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn header(&self, r: &impl Readable) -> Result<Header> {
        let bytes = r
            .get(&self.meta, META_HEADER)?
            .ok_or_else(|| SnapAcctError::Corrupt("no header".into()))?;
        from_postcard(&bytes, "header")
    }

    /// The commit sequence the index is "as of".
    pub fn accounted_seq(&self) -> Result<u64> {
        Ok(self.header(&self.db.read_tx())?.accounted_seq)
    }

    /// Advance `accounted_seq` without changing the index: for commits
    /// that touch no snapshot. A change that does carries its seq in the
    /// same call (see the module docs, "Replay").
    pub fn set_accounted_seq(&self, seq: u64) -> Result<()> {
        let mut tx = self.db.write_tx();
        self.advance_seq(&mut tx, seq)?;
        tx.commit()?;
        Ok(())
    }

    /// Write `seq` into the header inside `tx`. It never moves backwards:
    /// that would mean the caller is replaying history the index already
    /// holds, which it must skip instead.
    fn advance_seq(&self, tx: &mut fjall::SingleWriterWriteTx<'_>, seq: u64) -> Result<()> {
        let mut header = self.header(&*tx)?;
        if seq < header.accounted_seq {
            return Err(SnapAcctError::Invalid(format!(
                "accounted_seq would move back from {} to {seq}",
                header.accounted_seq
            )));
        }
        if seq != header.accounted_seq {
            header.accounted_seq = seq;
            tx.insert(&self.meta, META_HEADER, to_postcard(&header));
        }
        Ok(())
    }

    /// The chain of directory `dir_ino`, registering it on first use.
    pub fn register_chain(&self, dir_ino: u64) -> Result<u32> {
        let mut tx = self.db.write_tx();
        if let Some(v) = tx.get(&self.meta, ino_key(dir_ino))? {
            return from_postcard(&v, "chain id");
        }
        let chain: u32 = match tx.get(&self.meta, META_NEXT_CHAIN)? {
            Some(v) => from_postcard(&v, "next chain")?,
            None => 0,
        };
        if chain == u32::MAX {
            return Err(SnapAcctError::Invalid("chain ids exhausted".into()));
        }
        tx.insert(&self.meta, META_NEXT_CHAIN, to_postcard(&(chain + 1)));
        tx.insert(&self.meta, ino_key(dir_ino), to_postcard(&chain));
        tx.insert(
            &self.meta,
            chain_key(chain),
            to_postcard(&ChainRec {
                dir_ino,
                ..ChainRec::default()
            }),
        );
        tx.commit()?;
        Ok(chain)
    }

    pub fn chain_of(&self, dir_ino: u64) -> Result<Option<u32>> {
        self.meta
            .get(ino_key(dir_ino))?
            .map(|v| from_postcard(&v, "chain id"))
            .transpose()
    }

    /// Where snapshot `id` sits: `(chain, ord)`.
    pub fn locate(&self, id: &str) -> Result<Option<(u32, u32)>> {
        self.meta
            .get(id_key(id))?
            .map(|v| from_postcard(&v, "snapshot location"))
            .transpose()
    }

    // ------------------------------------------------------------ updates

    /// The chain's first snapshot (the chain must be empty): its full
    /// occurrences and `LSIZE`. `live` answers for chunks entering the
    /// index. Returns the snapshot's ordinal. `seq`, when given, becomes
    /// `accounted_seq` in the same transaction (as on every mutation
    /// below).
    #[allow(clippy::too_many_arguments)]
    pub fn apply_first(
        &self,
        chain: u32,
        id: &str,
        root: &str,
        lsize: u64,
        occurrences: impl IntoIterator<Item = Delta>,
        live: &mut dyn FnMut(&ChunkHash) -> bool,
        seq: Option<u64>,
    ) -> Result<u32> {
        let lsize = i64::try_from(lsize).map_err(|_| SnapAcctError::Invalid("lsize".into()))?;
        let mut ctx = ops::Ctx::new(self, seq)?;
        if ctx.head(chain)?.is_some() {
            return Err(SnapAcctError::Invalid(format!(
                "chain {chain} already has snapshots; apply_created takes deltas"
            )));
        }
        let ord = ctx.append(chain, id, root, lsize, occurrences, live)?;
        ctx.commit()?;
        Ok(ord)
    }

    /// A new head for a non-empty chain: `deltas` are
    /// `ChainWalk::step(head, new)`, `lsize_delta` is `LSIZE[new] −
    /// LSIZE[head]`.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_created(
        &self,
        chain: u32,
        id: &str,
        root: &str,
        lsize_delta: i64,
        deltas: impl IntoIterator<Item = Delta>,
        live: &mut dyn FnMut(&ChunkHash) -> bool,
        seq: Option<u64>,
    ) -> Result<u32> {
        let mut ctx = ops::Ctx::new(self, seq)?;
        if ctx.head(chain)?.is_none() {
            return Err(SnapAcctError::Invalid(format!(
                "chain {chain} is empty; apply_first takes occurrences"
            )));
        }
        let ord = ctx.append(chain, id, root, lsize_delta, deltas, live)?;
        ctx.commit()?;
        Ok(ord)
    }

    /// Delete snapshot `ord`. When it is the head and has a predecessor,
    /// `head_step` must be `ChainWalk::step(prev, head)` (the head's
    /// occurrence counts are the only ones stored, and this recovers the
    /// predecessor's); it is ignored otherwise.
    pub fn apply_deleted(
        &self,
        chain: u32,
        ord: u32,
        head_step: Option<Vec<Delta>>,
        seq: Option<u64>,
    ) -> Result<()> {
        let mut ctx = ops::Ctx::new(self, seq)?;
        ctx.delete(chain, ord, head_step)?;
        ctx.commit()
    }

    /// A snapshot that sorts before the chain's head (a late-applied
    /// row), or any rewrite of a chain's tail: rewind the chain to just
    /// after `after` (`None` = empty it) with `rewind` =
    /// `ChainWalk::step(after, head)` (unused when `after` is `None` or is
    /// the head), then append `suffix` in order — the new snapshot and
    /// every snapshot that followed `after`, each with its step from its
    /// predecessor in the new order (full occurrences for a first
    /// snapshot). One transaction. Returns the suffix's new ordinals.
    pub fn rederive_suffix(
        &self,
        chain: u32,
        after: Option<u32>,
        rewind: impl IntoIterator<Item = Delta>,
        suffix: Vec<NewSnapshot>,
        live: &mut dyn FnMut(&ChunkHash) -> bool,
        seq: Option<u64>,
    ) -> Result<Vec<u32>> {
        let mut ctx = ops::Ctx::new(self, seq)?;
        let head = ctx.head(chain)?;
        if head != after {
            match head {
                None => {
                    return Err(SnapAcctError::Invalid(format!(
                        "chain {chain} is empty; nothing to rewind to {after:?}"
                    )));
                }
                Some(_) => ctx.truncate(chain, after, rewind)?,
            }
            ctx.flush()?;
        }
        let mut ords = Vec::with_capacity(suffix.len());
        for snap in suffix {
            ords.push(ctx.append(
                chain,
                &snap.id,
                &snap.root,
                snap.lsize_delta,
                snap.deltas,
                live,
            )?);
            ctx.flush()?;
        }
        ctx.commit()?;
        Ok(ords)
    }

    /// Remove every snapshot of a chain (the chain stays registered).
    /// The recovery for a chain the caller cannot step, e.g. when the
    /// roots a rewind needs are gone: clear it, then re-apply it.
    pub fn clear_chain(&self, chain: u32, seq: Option<u64>) -> Result<()> {
        let mut ctx = ops::Ctx::new(self, seq)?;
        if ctx.head(chain)?.is_some() {
            ctx.truncate(chain, None, Vec::new())?;
        }
        ctx.commit()
    }

    /// The live-tree refresh: whether the live tree references `hash`.
    /// Chunks not in the index are not stored (only a tombstone's revival
    /// is recorded).
    pub fn set_live(&self, hash: ChunkHash, live: bool, seq: Option<u64>) -> Result<()> {
        self.set_live_many([(hash, live)], seq)
    }

    pub fn set_live_many(
        &self,
        updates: impl IntoIterator<Item = (ChunkHash, bool)>,
        seq: Option<u64>,
    ) -> Result<()> {
        let mut ctx = ops::Ctx::new(self, seq)?;
        for (hash, live) in updates {
            ctx.set_live(hash, live)?;
        }
        ctx.commit()
    }

    /// Drop tombstones older than `gc.horizon + gc interval`. Returns how
    /// many were dropped. Cost: the expired ones.
    pub fn expire_tombstones(&self) -> Result<u64> {
        let mut ctx = ops::Ctx::new(self, None)?;
        let dropped = ctx.expire_tombstones()?;
        ctx.commit()?;
        Ok(dropped)
    }

    // ------------------------------------------------------------ queries

    pub fn snap_numbers(&self, chain: u32, ord: u32) -> Result<Option<SnapNumbers>> {
        self.snap
            .get(snap_key(chain, ord))?
            .map(|v| Ok(numbers(chain, ord, from_postcard(&v, "snapshot record")?)))
            .transpose()
    }

    /// Every snapshot of a chain, in order. O(chain length).
    pub fn chain_snapshots(&self, chain: u32) -> Result<Vec<SnapNumbers>> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.prefix(&self.snap, chain.to_be_bytes()) {
            let (k, v) = guard.into_inner()?;
            let (chain, ord) = split_chain_ord(&k)?;
            out.push(numbers(chain, ord, from_postcard(&v, "snapshot record")?));
        }
        Ok(out)
    }

    pub fn chain_numbers(&self, chain: u32) -> Result<Option<ChainNumbers>> {
        let r = self.db.read_tx();
        let Some(rec) = r.get(&self.meta, chain_key(chain))? else {
            return Ok(None);
        };
        let rec: ChainRec = from_postcard(&rec, "chain record")?;
        let first = r.prefix(&self.snap, chain.to_be_bytes()).next();
        let head = r.prefix(&self.snap, chain.to_be_bytes()).next_back();
        let ord = |g: Option<fjall::Guard>| -> Result<Option<u32>> {
            g.map(|g| Ok(split_chain_ord(&g.key()?)?.1)).transpose()
        };
        Ok(Some(ChainNumbers {
            chain,
            dir_ino: rec.dir_ino,
            snapshots: rec.snapshots,
            first: ord(first)?,
            head: ord(head)?,
            used: rec.used,
            written: rec.written,
        }))
    }

    pub fn fs_breakdown(&self) -> Result<FsBreakdown> {
        let fs = fs_counters(&self.db.read_tx(), &self.meta)?;
        let total = (fs.unique.0 + fs.shared.0, fs.unique.1 + fs.shared.1);
        Ok(FsBreakdown {
            snapshots_total: total.into(),
            unique: fs.unique.into(),
            shared_only: fs.shared.into(),
            shared_with_live: fs.live.into(),
            awaiting_gc: fs.tomb.into(),
        })
    }

    pub fn awaiting_gc(&self) -> Result<Amount> {
        Ok(fs_counters(&self.db.read_tx(), &self.meta)?.tomb.into())
    }

    /// The index's bytes in on-disk tables: Σ of its keyspaces' segment
    /// files. Excludes the journal (preallocated, so it would dominate a
    /// small index) and data still in memtables, so it trails the most
    /// recent writes until they are flushed.
    pub fn footprint_bytes(&self) -> Result<u64> {
        Ok([
            &self.chunk,
            &self.birth,
            &self.death,
            &self.snap,
            &self.meta,
            &self.tomb,
            &self.lspill,
        ]
        .iter()
        .map(|ks| ks.inner().disk_space())
        .sum())
    }

    pub fn chunk_entry(&self, hash: &ChunkHash) -> Result<Option<ChunkEntry>> {
        self.chunk
            .get(hash.0)?
            .map(|v| decode_chunk(&v))
            .transpose()
    }

    /// `reclaim(D)`: the chunks every referrer of which is in `snapshots`
    /// and that are not live — what deleting exactly that set returns
    /// after GC. Cost: the runs born within each chain's `[min, max]` of
    /// D, plus that range's snapshot records. Unknown snapshots are
    /// refused.
    pub fn reclaim(&self, snapshots: &[(u32, u32)]) -> Result<Amount> {
        Ok(self.reclaim_listed(snapshots, None)?.0)
    }

    /// [`Self::reclaim`], and with `list_max` the hashes of the chunks it
    /// counted, sorted: a test aid (the `snapacct` harness scenario
    /// compares them with the chunks GC journals as deleted). The list is
    /// `None` when more than `list_max` chunks count: it is dropped as
    /// soon as it would pass that, so it never holds more.
    pub fn reclaim_listed(
        &self,
        snapshots: &[(u32, u32)],
        list_max: Option<usize>,
    ) -> Result<(Amount, Option<Vec<ChunkHash>>)> {
        let r = self.db.read_tx();
        let mut by_chain: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
        for &(chain, ord) in snapshots {
            by_chain.entry(chain).or_default().insert(ord);
        }
        // Per chain: D's range, the existing snapshots inside it that are
        // not in D (a run covering one is not covered), and the head
        // (where an open run ends).
        struct Span {
            min: u32,
            max: u32,
            gaps: Vec<u32>,
            head: u32,
        }
        let mut spans: HashMap<u32, Span> = HashMap::new();
        let mut candidates: HashSet<ChunkHash> = HashSet::new();
        for (&chain, ords) in &by_chain {
            let (min, max) = (*ords.first().unwrap(), *ords.last().unwrap());
            let mut gaps = Vec::new();
            let mut seen = 0usize;
            for guard in r.range(&self.snap, snap_key(chain, min)..=snap_key(chain, max)) {
                let (_, ord) = split_chain_ord(&guard.key()?)?;
                if ords.contains(&ord) {
                    seen += 1;
                } else {
                    gaps.push(ord);
                }
            }
            if seen != ords.len() {
                return Err(SnapAcctError::Invalid(format!(
                    "reclaim names {} snapshot(s) of chain {chain} the index does not have",
                    ords.len() - seen
                )));
            }
            let head = match r.prefix(&self.snap, chain.to_be_bytes()).next_back() {
                Some(g) => split_chain_ord(&g.key()?)?.1,
                None => unreachable!("a chain with snapshots in range has a head"),
            };
            for guard in r.range(
                &self.birth,
                ord_key(chain, min, &ChunkHash([0; 32]))
                    ..=ord_key(chain, max, &ChunkHash([0xff; 32])),
            ) {
                candidates.insert(ord_key_hash(&guard.key()?)?);
            }
            spans.insert(
                chain,
                Span {
                    min,
                    max,
                    gaps,
                    head,
                },
            );
        }
        let mut out = Amount::default();
        let mut listed = list_max.map(|_| Vec::new());
        for hash in candidates {
            let Some(v) = r.get(&self.chunk, hash.0)? else {
                return Err(SnapAcctError::Corrupt(format!(
                    "birth of {} without a chunk entry",
                    hash.to_hex()
                )));
            };
            let entry = decode_chunk(&v)?;
            if entry.live {
                continue;
            }
            let covered = entry.runs.iter().all(|run| {
                let Some(span) = spans.get(&run.chain) else {
                    return false;
                };
                let last = if run.is_open() { span.head } else { run.last };
                let gap = span.gaps.partition_point(|&g| g < run.first);
                run.first >= span.min
                    && last <= span.max
                    && span.gaps.get(gap).is_none_or(|&g| g > last)
            });
            if covered {
                out.bytes += entry.size;
                out.chunks += 1;
                if let Some(list) = &mut listed {
                    if list_max.is_some_and(|max| list.len() >= max) {
                        listed = None;
                    } else {
                        list.push(hash);
                    }
                }
            }
        }
        if let Some(listed) = &mut listed {
            listed.sort_unstable_by_key(|hash| hash.0);
        }
        Ok((out, listed))
    }

    /// Every indexed chunk, in hash order. O(index): `--verify` and the
    /// live refresh's fallback only.
    pub fn for_each_chunk(
        &self,
        mut f: impl FnMut(ChunkHash, ChunkEntry) -> Result<()>,
    ) -> Result<()> {
        let r = self.db.read_tx();
        for guard in r.iter(&self.chunk) {
            let (k, v) = guard.into_inner()?;
            let hash: [u8; 32] = k
                .as_ref()
                .try_into()
                .map_err(|_| SnapAcctError::Corrupt("short chunk key".into()))?;
            f(ChunkHash(hash), decode_chunk(&v)?)?;
        }
        Ok(())
    }

    /// Every registered chain as `(chain, dir ino)`, in chain order.
    pub fn chains(&self) -> Result<Vec<(u32, u64)>> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.prefix(&self.meta, b"chain/") {
            let (k, v) = guard.into_inner()?;
            let chain: [u8; 4] = k
                .get(6..)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| SnapAcctError::Corrupt("short chain key".into()))?;
            let rec: ChainRec = from_postcard(&v, "chain record")?;
            out.push((u32::from_be_bytes(chain), rec.dir_ino));
        }
        Ok(out)
    }

    // ------------------------------------------------- caller-owned state

    /// A value the index's owner keeps next to it (its build cursor, the
    /// commit its live flags are as of): stored in `snapacct_meta` under
    /// `aux/<key>`, so it is wiped together with the index.
    pub fn aux(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.meta.get(aux_key(key))?.map(|v| v.to_vec()))
    }

    pub fn put_aux(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut tx = self.db.write_tx();
        tx.insert(&self.meta, aux_key(key), value.to_vec());
        tx.commit()?;
        Ok(())
    }

    pub fn remove_aux(&self, key: &str) -> Result<()> {
        let mut tx = self.db.write_tx();
        tx.remove(&self.meta, aux_key(key));
        tx.commit()?;
        Ok(())
    }

    /// Whether `spill` is recorded as a live spilled list.
    pub fn live_spill_known(&self, spill: &ChunkHash) -> Result<bool> {
        Ok(self.lspill.get(spill_key(spill))?.is_some())
    }

    /// Record `spill` as a list the live tree references, with its
    /// `members` (idempotent).
    pub fn record_live_spill(
        &self,
        spill: &ChunkHash,
        members: impl IntoIterator<Item = ChunkHash>,
    ) -> Result<()> {
        let mut tx = self.db.write_tx();
        for member in members {
            tx.insert(&self.lspill, member_key(&member, spill), Vec::new());
        }
        tx.insert(&self.lspill, spill_key(spill), Vec::new());
        tx.commit()?;
        Ok(())
    }

    /// The live tree no longer references `spill` (idempotent).
    pub fn forget_live_spill(
        &self,
        spill: &ChunkHash,
        members: impl IntoIterator<Item = ChunkHash>,
    ) -> Result<()> {
        let mut tx = self.db.write_tx();
        for member in members {
            tx.remove(&self.lspill, member_key(&member, spill));
        }
        tx.remove(&self.lspill, spill_key(spill));
        tx.commit()?;
        Ok(())
    }

    /// Whether some recorded live spilled list has `chunk` as a member.
    pub fn in_live_spill(&self, chunk: &ChunkHash) -> Result<bool> {
        let mut prefix = Vec::with_capacity(33);
        prefix.push(b'm');
        prefix.extend_from_slice(&chunk.0);
        Ok(self
            .db
            .read_tx()
            .prefix(&self.lspill, prefix)
            .next()
            .is_some())
    }

    /// The recorded members of live spilled list `spill`, by a scan of
    /// every recorded member: the fallback when the list itself can no
    /// longer be read.
    pub fn live_spill_members(&self, spill: &ChunkHash) -> Result<Vec<ChunkHash>> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.prefix(&self.lspill, b"m") {
            let key = guard.key()?;
            if key.len() == 65 && key[33..] == spill.0 {
                out.push(ChunkHash(key[1..33].try_into().expect("32 bytes")));
            }
        }
        Ok(out)
    }

    /// Every recorded live spilled list.
    pub fn live_spills(&self) -> Result<HashSet<ChunkHash>> {
        let r = self.db.read_tx();
        let mut out = HashSet::new();
        for guard in r.prefix(&self.lspill, b"s") {
            let key = guard.key()?;
            if key.len() == 33 {
                out.insert(ChunkHash(key[1..].try_into().expect("32 bytes")));
            }
        }
        Ok(out)
    }

    /// Forget every recorded live spilled list not in `keep`, members
    /// included: one scan, committed in batches.
    pub fn retain_live_spills(&self, keep: &HashSet<ChunkHash>) -> Result<()> {
        let spill_of = |key: &[u8]| -> Option<ChunkHash> {
            match (key.first(), key.len()) {
                (Some(b's'), 33) => Some(ChunkHash(key[1..].try_into().expect("32 bytes"))),
                (Some(b'm'), 65) => Some(ChunkHash(key[33..].try_into().expect("32 bytes"))),
                _ => None,
            }
        };
        let stale: Vec<Vec<u8>> = {
            let r = self.db.read_tx();
            let mut stale = Vec::new();
            for guard in r.iter(&self.lspill) {
                let key = guard.key()?;
                if spill_of(&key).is_none_or(|spill| !keep.contains(&spill)) {
                    stale.push(key.to_vec());
                }
            }
            stale
        };
        for batch in stale.chunks(10_000) {
            let mut tx = self.db.write_tx();
            for key in batch {
                tx.remove(&self.lspill, key.clone());
            }
            tx.commit()?;
        }
        Ok(())
    }

    /// Overwrite one chunk entry as it is, bypassing every rule: tests
    /// corrupt the index with it to see `--verify` notice.
    #[cfg(test)]
    pub(crate) fn overwrite_chunk_entry(&self, hash: &ChunkHash, entry: &ChunkEntry) -> Result<()> {
        let mut tx = self.db.write_tx();
        tx.insert(&self.chunk, hash.0, encode_chunk(entry));
        tx.commit()?;
        Ok(())
    }

    /// A full scan checking every structural invariant and recomputing
    /// every counter from the chunk entries: the index's self-check
    /// (tests, `--verify`). O(index).
    pub fn check_structure(&self) -> Result<(), String> {
        ops::check_structure(self).map_err(|e| match e {
            SnapAcctError::Corrupt(s) => s,
            other => other.to_string(),
        })
    }
}

/// Occurrences as the all-positive deltas [`SnapAcct::apply_first`]
/// takes.
pub fn first_deltas(occurrences: &Occurrences) -> impl Iterator<Item = Delta> + '_ {
    occurrences.iter_sized().map(|(hash, size, count)| Delta {
        hash: *hash,
        delta: count as i64,
        size_bytes: size,
    })
}

fn numbers(chain: u32, ord: u32, rec: SnapRec) -> SnapNumbers {
    SnapNumbers {
        chain,
        ord,
        id: rec.id,
        root: rec.root,
        used: rec.used,
        written: rec.written,
        refer: rec.refer,
        lsize: rec.lsize,
    }
}

fn aux_key(key: &str) -> Vec<u8> {
    let mut out = b"aux/".to_vec();
    out.extend_from_slice(key.as_bytes());
    out
}

fn spill_key(spill: &ChunkHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.push(b's');
    key.extend_from_slice(&spill.0);
    key
}

fn member_key(member: &ChunkHash, spill: &ChunkHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(65);
    key.push(b'm');
    key.extend_from_slice(&member.0);
    key.extend_from_slice(&spill.0);
    key
}

fn fs_counters(r: &impl Readable, meta: &SingleWriterTxKeyspace) -> Result<FsCounters> {
    match r.get(meta, META_FS)? {
        Some(v) => from_postcard(&v, "fs counters"),
        None => Ok(FsCounters::default()),
    }
}
