//! Fix "capture under an epoch hold" (plan 30 §M7 × §M10): a model of
//! file content in the simulation — chunks, dirty on the node that wrote
//! them until that node's upload pass puts them in S3.
//!
//! The daemon's rule is that a manifest reaches the log only once every
//! chunk it names is in S3 (`store::held`'s module doc). A non-owner
//! forwards its manifest at once, saying which chunks are still pending
//! on it; the sequencer enrolls them as remote pending rows before it
//! executes the op, its ship plan defers the transaction (and whatever
//! depends on it) until the rows are acked, and everything else ships.
//! Inside a continuation epoch nothing reaches S3 at all, so every epoch
//! write's chunks sit on a member until the close — and a member that
//! dies with the only copy leaves the hold owner's transaction deferred
//! until it returns, or until the operator drops it
//! (`repair drop-held --remote`).
//!
//! What is modelled, and where the driver hooks it in:
//! - a client `Step::Write(name)` names one fresh chunk, dirty on its
//!   node ([`ChunkWorld::write`] plus the node's own `pending_upload`
//!   row), and submits `MutateOp::SetManifest` like the FUSE close does;
//! - a `MutateRequest` carrying a manifest enrolls, on the receiver, the
//!   named chunks still pending on the sender ([`ChunkWorld::pending_on`]
//!   asks the sender's replica, as `cli::forwarded_pending_chunks` asks
//!   the local one — a replay by rid after a deposition carries the
//!   deposed owner's remote rows on the same way);
//! - `Action::UploadDirtyChunks` runs [`ChunkWorld::upload_pass`]: with
//!   S3 reachable, this node's own dirty chunks go up and their rows are
//!   acked; a remote row is acked when its chunk is in S3 (the daemon's
//!   fallback check; the forwarder's report is not modelled);
//! - every segment PUT is checked at PUT time: a `WriteManifest` naming
//!   a chunk S3 does not hold is a violation ([`ChunkWorld::check_segment`]).
//!
//! A node that dies for good keeps its dirty set (its disk), and no one
//! else has the bytes: the run's end applies the operator procedure
//! (`run.rs`).

use constellation_authority::{segment, NodeId};
use constellation_fs_core::{ChunkHash, ChunkInfo, ChunkLayout, Ino, Manifest};
use constellation_meta::{LogRecord, Meta};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// One chunk per write, one write per file: 4 KiB.
pub const CHUNK_SIZE: u32 = 4096;

#[derive(Default)]
pub struct ChunkWorld {
    /// What the bucket holds.
    s3: Mutex<HashSet<ChunkHash>>,
    /// Per node, the chunks only its disk holds.
    dirty: Mutex<HashMap<NodeId, HashSet<ChunkHash>>>,
    /// Each node's replica (kept across restarts with its journal), for
    /// what is pending on a forward's sender.
    metas: Mutex<HashMap<NodeId, Arc<Meta>>>,
    /// Nodes crashed for good (`FaultKind::CrashEpochMember` with no
    /// restart): their dirty chunks never come.
    pub gone: Mutex<BTreeSet<NodeId>>,
    /// Segments that named a chunk S3 did not hold when they were put.
    pub violations: Mutex<Vec<String>>,
    pub writes: AtomicU64,
    pub uploaded: AtomicU64,
    pub remote_enrolled: AtomicU64,
    pub remote_acked: AtomicU64,
    /// Ship plans seen deferring a transaction (sampled by the run).
    pub deferred_seen: AtomicU64,
    /// Transactions dropped by the run's end-of-run operator procedure.
    pub remote_dropped: AtomicU64,
    /// Own pending rows found without bytes anywhere (a replay's adopted
    /// manifest of a departed node's chunk): marked unrecoverable.
    pub poisoned: AtomicU64,
    /// Chunk metered-own-rows: the metered upload hold — a round's pass
    /// leaves a node's own chunk dirty until it is this many ms old; only
    /// `Action::UploadAwaited` (an op's or an observed position's
    /// durability need, which the hold exempts) puts it up sooner. 0: no
    /// hold.
    pub hold_ms: AtomicU64,
    /// When each chunk was written (for the hold).
    written_at: Mutex<HashMap<ChunkHash, tokio::time::Instant>>,
    /// Own chunks a round's pass left held.
    pub held_back: AtomicU64,
    /// Own chunks `Action::UploadAwaited` put up past the hold.
    pub awaited_uploaded: AtomicU64,
}

impl ChunkWorld {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, node: NodeId, meta: Arc<Meta>) {
        self.metas.lock().unwrap().insert(node, meta);
    }

    /// The manifest of a write naming `hash`.
    pub fn manifest(hash: ChunkHash) -> Vec<u8> {
        Manifest {
            layout: ChunkLayout::new(CHUNK_SIZE),
            file_len: CHUNK_SIZE as u64,
            chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, hash)])),
        }
        .encode()
    }

    /// `node` wrote `hash`: dirty there, nowhere else.
    pub fn write(&self, node: NodeId, hash: ChunkHash) {
        self.dirty
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .insert(hash);
        self.written_at
            .lock()
            .unwrap()
            .insert(hash, tokio::time::Instant::now());
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn in_s3(&self, hash: &ChunkHash) -> bool {
        self.s3.lock().unwrap().contains(hash)
    }

    pub fn dirty_on(&self, node: NodeId) -> usize {
        self.dirty.lock().unwrap().get(&node).map_or(0, |s| s.len())
    }

    /// The chunks a manifest names.
    pub fn named(manifest: &[u8]) -> Vec<ChunkHash> {
        match Manifest::decode(manifest).map(|m| m.chunks) {
            Ok(ChunkInfo::Inline(chunks)) => chunks.into_values().collect(),
            Ok(ChunkInfo::Spilled(blob)) => vec![blob],
            Err(_) => Vec::new(),
        }
    }

    /// Of the chunks `manifest` names, those still pending on `sender`
    /// (its own dirty rows or remote rows it awaits itself): what the
    /// daemon's forward says (`cli::forwarded_pending_chunks`).
    pub fn pending_on(&self, sender: NodeId, manifest: &[u8]) -> Vec<ChunkHash> {
        let Some(meta) = self.metas.lock().unwrap().get(&sender).cloned() else {
            return Vec::new();
        };
        Self::named(manifest)
            .into_iter()
            .filter(|h| meta.upload_pending_for_hash(h).unwrap_or(true))
            .collect()
    }

    /// The receiver of a forwarded manifest enrolls the sender's pending
    /// chunks before the op executes (`authority_driver.rs`'s
    /// `MutateRequest` arm). Returns how many it enrolled.
    pub fn enroll_forwarded(
        &self,
        receiver: &Meta,
        sender: NodeId,
        ino: Ino,
        manifest: &[u8],
    ) -> usize {
        let pending: Vec<ChunkHash> = self
            .pending_on(sender, manifest)
            .into_iter()
            .filter(|h| !self.in_s3(h))
            .collect();
        if pending.is_empty() {
            return 0;
        }
        receiver
            .enroll_remote_chunks(ino, &pending, sender)
            .expect("enroll remote chunks");
        self.remote_enrolled
            .fetch_add(pending.len() as u64, Ordering::Relaxed);
        pending.len()
    }

    /// `node`'s upload pass (`cli::upload_dirty_chunks`), for `ino` only
    /// or everything pending. With S3 reachable: its own dirty chunks go
    /// up and their rows are acked; a remote row is acked once its chunk
    /// is in S3 (whoever put it there). With S3 cut nothing moves.
    pub fn upload_pass(&self, node: NodeId, meta: &Meta, ino: Option<Ino>, s3_up: bool) {
        self.pass(node, meta, ino, s3_up, false)
    }

    /// `Action::UploadAwaited` (chunk metered-own-rows): `node`'s pass
    /// for `inos`, past the metered hold.
    pub fn upload_awaited(&self, node: NodeId, meta: &Meta, inos: &[Ino], s3_up: bool) {
        for ino in inos {
            self.pass(node, meta, Some(*ino), s3_up, true)
        }
    }

    /// A complete pass (a barrier's, a flush's): past the metered hold.
    pub fn upload_forced(&self, node: NodeId, meta: &Meta, ino: Option<Ino>, s3_up: bool) {
        self.pass(node, meta, ino, s3_up, true)
    }

    fn pass(&self, node: NodeId, meta: &Meta, ino: Option<Ino>, s3_up: bool, forced: bool) {
        if !s3_up {
            return;
        }
        let hold = std::time::Duration::from_millis(self.hold_ms.load(Ordering::Relaxed));
        let rows = meta.pending_uploads().unwrap_or_default();
        for (hash, row_ino) in rows {
            if ino.is_some_and(|i| i != row_ino) {
                continue;
            }
            let on_disk = self
                .dirty
                .lock()
                .unwrap()
                .get(&node)
                .is_some_and(|d| d.contains(&hash));
            // As the daemon's pass: a remote row is awaited only when the
            // bytes are not here (`upload.rs` reads the cache first). A
            // deposed holder's replay can mark this node's own chunk
            // remote from the deposed node, whose row is marked remote
            // from this one in turn: uploading what is on disk breaks
            // that cycle.
            if !on_disk && meta.remote_chunk(&hash, row_ino).ok().flatten().is_some() {
                if self.in_s3(&hash) {
                    let acked = meta.ack_remote_chunks(&[hash]).expect("ack remote");
                    self.remote_acked
                        .fetch_add(acked.len() as u64, Ordering::Relaxed);
                }
                continue;
            }
            // The metered hold: a round's pass leaves a young chunk of
            // this node's own on its disk.
            let young = !hold.is_zero()
                && self
                    .written_at
                    .lock()
                    .unwrap()
                    .get(&hash)
                    .is_some_and(|t| t.elapsed() < hold);
            if young && on_disk && !self.in_s3(&hash) {
                if forced {
                    self.awaited_uploaded.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.held_back.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            // This node's own chunk: on its disk, or already up (a row
            // re-enrolled for content that uploaded under another inode).
            let mine = self
                .dirty
                .lock()
                .unwrap()
                .get_mut(&node)
                .is_some_and(|d| d.remove(&hash));
            if mine || self.in_s3(&hash) {
                if self.s3.lock().unwrap().insert(hash) {
                    self.uploaded.fetch_add(1, Ordering::Relaxed);
                }
                meta.ack_upload(&hash, row_ino).expect("ack upload");
            } else {
                // Not on this node's disk and not in S3: the row was
                // enrolled here by a replay's adopted records
                // (`apply_adopted_records`) for a chunk only a dead node
                // had. The daemon's pass finds it missing from the local
                // cache and marks it unrecoverable (`store::held`): the
                // transaction is held, and the operator drops it.
                meta.note_unrecoverable_chunks(&[(hash, row_ino)], false)
                    .expect("poison");
                self.poisoned.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Whether `hash`, awaited by someone from `uploader`, is never
    /// coming: not in S3, and the uploader is gone for good, or has
    /// nothing of it (no pending row: it dropped it), or awaits it in
    /// turn from a node that is hopeless (a deposed hold owner's replay
    /// forwarded its remote row on). What the operator would conclude
    /// from `status` on each node in the chain.
    pub fn hopeless(&self, hash: &ChunkHash, uploader: NodeId) -> bool {
        let mut node = uploader;
        for _ in 0..8 {
            if self.in_s3(hash) {
                return false;
            }
            if self.gone.lock().unwrap().contains(&node) {
                return true;
            }
            let Some(meta) = self.metas.lock().unwrap().get(&node).cloned() else {
                return true;
            };
            if !meta.upload_pending_for_hash(hash).unwrap_or(false) {
                // Nothing pending there: not its own dirty chunk either.
                return self
                    .dirty
                    .lock()
                    .unwrap()
                    .get(&node)
                    .is_none_or(|d| !d.contains(hash));
            }
            match meta
                .remote_chunks()
                .unwrap_or_default()
                .into_iter()
                .find(|r| r.hash == *hash)
            {
                // Its own: it uploads once its S3 is back.
                None => return false,
                Some(r) => node = r.node,
            }
        }
        false
    }

    /// A segment about to be put by `node`: every chunk its manifests
    /// name must be in S3 already.
    pub fn check_segment(&self, node: NodeId, seq: u64, payload: &[u8]) {
        let Ok(seg) = segment::decode(payload) else {
            return;
        };
        for rec in &seg.records {
            let LogRecord::WriteManifest { ino, manifest, .. } = rec else {
                continue;
            };
            for hash in Self::named(manifest) {
                if !self.in_s3(&hash) {
                    self.violations.lock().unwrap().push(format!(
                        "node {node} put segment {seq} (epoch {}) with a manifest of inode \
                         {ino:#x} naming chunk {} that is not in S3",
                        seg.epoch,
                        &hash.to_hex()[..12]
                    ));
                }
            }
        }
    }
}
