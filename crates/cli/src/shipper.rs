//! Metadata log sync (DESIGN.md §4): one component both **tails** the
//! shared S3 log (applying other nodes' segments to the local replica)
//! and **ships** the local journal as CAS-created segments. The CAS
//! collision on a sequence number is the multi-writer conflict
//! detector: the loser applies the winner's segment and retries at the
//! next sequence (DESIGN.md "losers re-tail and retry").
//!
//! From phase 3 shipping is **lease-gated**: a segment may only be
//! written while this node holds an unexpired lease on the partition,
//! and it is stamped with that lease's epoch. Tailing records the
//! highest epoch applied; a segment arriving with a lower epoch is a
//! deposed holder's late write and is skipped as a fencing violation.
//!
//! The leaseless convergence path (foreign records that touch pending
//! local state are skipped via [`TouchSet`], ours being later in the
//! global log) is retained as a safety net. With leases it is
//! unreachable in normal operation — the harness asserts the conflict
//! counter stays at zero — but a backend without `If-Match`, or a
//! future relaxed mode, still needs deterministic convergence.

use crate::lease::{LeaseKeeper, TailedToHead};
use anyhow::{bail, Context, Result};
use constellation_meta::replay::TouchSet;
use constellation_meta::{LogRecord, MetaStore, SqliteMeta};
use constellation_store_s3::LogStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Checkpoint after this many shipped segments.
const CHECKPOINT_EVERY: u64 = 32;
/// Max journal records per segment.
const SEGMENT_BATCH: usize = 10_000;

/// What one log segment holds: its origin node, the lease epoch under
/// which it was written, and a batch of records. Versioned; old bare
/// arrays decode as node 0 / epoch 0, as do pre-lease envelopes.
#[derive(Serialize, Deserialize)]
struct SegmentEnvelope {
    v: u32,
    node: u64,
    #[serde(default)]
    epoch: u64,
    records: Vec<LogRecord>,
}

struct Segment {
    node: u64,
    epoch: u64,
    records: Vec<LogRecord>,
}

fn encode(node: u64, epoch: u64, records: &[LogRecord]) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&SegmentEnvelope {
        v: 2,
        node,
        epoch,
        records: records.to_vec(),
    })?)
}

fn decode(payload: &[u8]) -> Result<Segment> {
    if let Ok(env) = serde_json::from_slice::<SegmentEnvelope>(payload) {
        return Ok(Segment {
            node: env.node,
            epoch: env.epoch,
            records: env.records,
        });
    }
    // Pre-envelope segments: a bare JSON array from the genesis node.
    let records: Vec<LogRecord> = serde_json::from_slice(payload)?;
    Ok(Segment {
        node: 0,
        epoch: 0,
        records,
    })
}

pub struct Shipper {
    meta: Arc<SqliteMeta>,
    log: LogStore,
    node_id: u64,
    /// Next sequence number not yet seen (neither shipped nor tailed).
    next_seq: u64,
    /// Highest lease epoch observed in an applied segment: the fence.
    max_epoch: u64,
    shipped_since_ckpt: u64,
    /// Live spool observability shared with the control API.
    pub spool: Arc<std::sync::Mutex<SpoolInfo>>,
}

/// Snapshot of sync progress (updated on every sync attempt).
#[derive(Default, Clone)]
pub struct SpoolInfo {
    /// Highest log sequence shipped or applied.
    pub head_seq: u64,
    /// Foreign records skipped because pending local ops won.
    pub conflicts: u64,
    /// Segments skipped because their lease epoch was already superseded.
    pub fenced: u64,
    pub last_error: Option<String>,
}

impl Shipper {
    /// Attach to an existing local replica. `applied_seq` is the log
    /// position the replica covers (from the local kv store); segments
    /// beyond it are tailed on the first `sync`.
    pub fn attach(meta: Arc<SqliteMeta>, log: LogStore, node_id: u64) -> Result<Self> {
        let applied = meta.applied_seq()?;
        Ok(Self {
            meta,
            log,
            node_id,
            next_seq: applied + 1,
            max_epoch: 0,
            shipped_since_ckpt: 0,
            spool: Arc::new(std::sync::Mutex::new(SpoolInfo {
                head_seq: applied,
                ..Default::default()
            })),
        })
    }

    /// One full sync round: tail new segments, then ship the journal
    /// until it drains (re-tailing after every CAS collision).
    ///
    /// `lease` gates shipping: without a usable lease the journal simply
    /// stays put (tailing always proceeds — reads never need authority).
    pub async fn sync(&mut self, lease: &LeaseKeeper) -> Result<()> {
        loop {
            self.tail().await?;
            if !self.ship_one(lease).await? {
                return Ok(());
            }
        }
    }

    /// Tail-only round: applies the shared log up to head and returns
    /// the witness that makes a lease takeover legal (DESIGN.md §4:
    /// "takeover is legal only after applying everything the holder
    /// flushed").
    pub async fn tail_to_head(&mut self) -> Result<TailedToHead> {
        self.tail().await?;
        Ok(TailedToHead::witness())
    }

    /// Apply all segments at `next_seq..` (contiguous run only: a gap
    /// means a concurrent LIST raced a PUT; the next round gets it).
    async fn tail(&mut self) -> Result<()> {
        let seqs = self.log.list_segments_from(self.next_seq).await?;
        for seq in seqs {
            if seq != self.next_seq {
                break;
            }
            self.apply_segment(seq).await?;
        }
        Ok(())
    }

    async fn apply_segment(&mut self, seq: u64) -> Result<()> {
        let seg = decode(&self.log.get_segment(seq).await?)?;
        if seg.node == self.node_id {
            // Our own segment from a previous life: the PUT succeeded
            // but the response (or the ack) was lost. The journal head
            // must match; ack it instead of re-applying.
            let journal = self.meta.take_journal(seg.records.len())?;
            let matches = journal.len() == seg.records.len()
                && journal.iter().map(|(_, r)| r).eq(seg.records.iter());
            if !matches {
                bail!(
                    "segment {seq} claims our node id {} but does not match \
                     the journal head: state dir reuse or id collision",
                    self.node_id
                );
            }
            self.meta.ack_journal_at(journal.last().unwrap().0, seq)?;
            tracing::info!(
                seq,
                records = seg.records.len(),
                "recovered unacked segment"
            );
        } else if seg.epoch > 0 && seg.epoch < self.max_epoch {
            // A deposed holder flushed after losing the lease. The
            // sequence CAS stops most of this; the epoch catches the
            // rest. Skipping is the only safe choice: the current holder
            // built its state without these records.
            self.spool.lock().unwrap().fenced += 1;
            tracing::error!(
                seq,
                node = seg.node,
                epoch = seg.epoch,
                max_epoch = self.max_epoch,
                records = seg.records.len(),
                "FENCING VIOLATION: segment from a superseded lease epoch; skipping"
            );
            self.meta.set_applied_seq(seq)?;
        } else {
            let pending =
                TouchSet::from_records(self.meta.take_journal(usize::MAX)?.iter().map(|(_, r)| r));
            let skipped = self.meta.apply_foreign(&seg.records, &pending)?;
            if skipped > 0 {
                self.spool.lock().unwrap().conflicts += skipped as u64;
            }
            tracing::debug!(
                seq,
                node = seg.node,
                epoch = seg.epoch,
                records = seg.records.len(),
                skipped,
                "applied foreign segment"
            );
            self.meta.set_applied_seq(seq)?;
        }
        self.max_epoch = self.max_epoch.max(seg.epoch);
        self.advance(seq);
        Ok(())
    }

    /// Ship one journal batch at `next_seq`. Returns true if another
    /// round is needed (more records pending, or a CAS collision).
    async fn ship_one(&mut self, lease: &LeaseKeeper) -> Result<bool> {
        let Some(epoch) = lease.ship_epoch() else {
            // No write authority: the journal waits. Deposed nodes stay
            // here forever by design (phase-4 reintegration).
            return Ok(false);
        };
        let batch = self.meta.take_journal(SEGMENT_BATCH)?;
        if batch.is_empty() {
            return Ok(false);
        }
        let records: Vec<LogRecord> = batch.iter().map(|(_, r)| r.clone()).collect();
        let payload = encode(self.node_id, epoch, &records)?;
        match self.log.put_segment(self.next_seq, &payload).await {
            Ok(()) => {}
            Err(constellation_store_s3::StoreError::AlreadyExists) => {
                // Lost the race for this sequence number (or our own
                // earlier PUT's response was lost). The next round's
                // tail applies whatever is there and re-ships.
                return Ok(true);
            }
            Err(e) => return Err(e).context("shipping log segment"),
        }
        self.meta
            .ack_journal_at(batch.last().unwrap().0, self.next_seq)?;
        tracing::debug!(
            seq = self.next_seq,
            epoch,
            records = records.len(),
            "shipped log segment"
        );
        self.max_epoch = self.max_epoch.max(epoch);
        self.advance(self.next_seq);
        self.shipped_since_ckpt += 1;
        if self.shipped_since_ckpt >= CHECKPOINT_EVERY {
            self.checkpoint().await?;
        }
        Ok(true)
    }

    fn advance(&mut self, seq: u64) {
        {
            let mut spool = self.spool.lock().unwrap();
            spool.head_seq = seq;
            spool.last_error = None;
        }
        self.next_seq = seq + 1;
    }

    /// Snapshot the local DB as a checkpoint covering the log up to the
    /// last sequence this replica has seen.
    pub async fn checkpoint(&mut self) -> Result<()> {
        if self.next_seq == 1 {
            return Ok(()); // nothing seen, nothing to cover
        }
        let snap = self.meta.snapshot()?;
        let covered = self.next_seq - 1;
        self.log.put_checkpoint(covered, &snap).await?;
        self.shipped_since_ckpt = 0;
        tracing::info!(
            seq = covered,
            bytes = snap.len(),
            "wrote metadata checkpoint"
        );
        Ok(())
    }

    /// Final sync + checkpoint on clean unmount.
    pub async fn shutdown(&mut self, lease: &LeaseKeeper) -> Result<()> {
        self.sync(lease).await?;
        if self.shipped_since_ckpt > 0 {
            self.checkpoint().await?;
        }
        Ok(())
    }

    pub fn journal_backlog(&self) -> u64 {
        MetaStore::journal_len(&*self.meta).unwrap_or(0)
    }
}

/// Acquire the partition lease if it is free, tailing the shared log to
/// head first when this would be a takeover from another node. Returns
/// false when a live foreign holder still owns it (the caller waits and
/// retries). This is the *only* acquisition path, which is what makes
/// the takeover ordering rule structural.
pub async fn acquire_lease(ship: &mut Shipper, keeper: &mut LeaseKeeper) -> Result<bool> {
    let plan = keeper.classify().await?;
    if let crate::lease::Plan::Busy {
        holder,
        expires_in_ms,
    } = &plan
    {
        tracing::debug!(
            holder,
            expires_in_ms,
            "partition lease held by another node"
        );
    }
    let tailed = if plan.needs_tail() {
        Some(ship.tail_to_head().await?)
    } else {
        None
    };
    keeper.commit(plan, tailed).await
}

/// Build a fresh local replica from S3: latest checkpoint (if any) plus
/// replay of newer segments. Used when the state dir has no metadata DB.
pub async fn bootstrap(db_path: &std::path::Path, log: &LogStore) -> Result<()> {
    let from_seq = match log.get_latest_checkpoint().await? {
        Some((seq, snapshot)) => {
            std::fs::write(db_path, &snapshot).context("writing checkpoint snapshot")?;
            tracing::info!(seq, "restored metadata checkpoint");
            seq
        }
        None => 0,
    };
    let meta = SqliteMeta::open(db_path)?;
    let mut replayed = 0usize;
    let mut applied = from_seq;
    for seq in log.list_segments_from(from_seq + 1).await? {
        if seq != applied + 1 {
            break; // gap: a racing PUT; the mount's tailer catches up
        }
        let seg = decode(&log.get_segment(seq).await?)?;
        replayed += seg.records.len();
        meta.apply_records(&seg.records)
            .with_context(|| format!("replaying log segment {seq}"))?;
        applied = seq;
    }
    meta.set_applied_seq(applied)?;
    // No process holds unlinked-but-open files on a brand-new replica.
    for ino in meta.orphans()? {
        meta.reap_orphan(ino)?;
    }
    tracing::info!(from_seq, replayed, "bootstrapped metadata replica");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseKeeper;
    use constellation_meta::MetaStore;
    use constellation_store_s3::lease::LeaseMode;
    use constellation_store_s3::LeaseStore;
    use object_store::memory::InMemory;
    use std::sync::Arc as StdArc;

    struct Node {
        meta: Arc<SqliteMeta>,
        ship: Shipper,
        lease: LeaseKeeper,
    }

    fn node(store: &StdArc<InMemory>, id: u64) -> Node {
        let meta = Arc::new(SqliteMeta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let log = LogStore::new(store.clone());
        let ship = Shipper::attach(meta.clone(), log, id).unwrap();
        let lease = LeaseKeeper::new(
            LeaseStore::new(
                store.clone(),
                constellation_store_s3::log::PARTITION,
                LeaseMode::Cas,
            ),
            id,
        );
        Node { meta, ship, lease }
    }

    impl Node {
        /// What the daemon's sync task does per round: take authority if
        /// it is available, then tail + ship.
        async fn sync(&mut self) {
            acquire_lease(&mut self.ship, &mut self.lease)
                .await
                .unwrap();
            self.ship.sync(&self.lease).await.unwrap();
        }

        async fn release(&mut self) {
            self.lease.release().await.unwrap();
        }
    }

    fn names(meta: &SqliteMeta, parent: u64) -> Vec<(String, u64)> {
        let mut v: Vec<_> = meta
            .readdir(parent)
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.ino))
            .collect();
        v.sort();
        v
    }

    async fn segment(store: &StdArc<InMemory>, seq: u64) -> Segment {
        let log = LogStore::new(store.clone());
        decode(&log.get_segment(seq).await.unwrap()).unwrap()
    }

    /// Two writers on disjoint names: both replicas converge to the
    /// union, and ino prefixes never collide. With leases they take
    /// turns; each release/acquire bumps the epoch.
    #[tokio::test]
    async fn two_nodes_disjoint_converge() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let da = a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        let db = b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        assert_ne!(da.ino >> 40, db.ino >> 40, "ino prefixes must differ");

        a.sync().await; // ships A's segment under epoch 1
        a.release().await; // A goes idle
        b.sync().await; // B takes over (epoch 2), tails A, ships
        a.sync().await; // A tails B

        assert_eq!(names(&a.meta, 1), names(&b.meta, 1));
        assert_eq!(names(&a.meta, 1).len(), 2);
        assert_eq!(segment(&store, 1).await.epoch, 1);
        assert_eq!(
            segment(&store, 2).await.epoch,
            2,
            "handover bumps the epoch"
        );
        assert_eq!(a.ship.spool.lock().unwrap().conflicts, 0);
        assert_eq!(b.ship.spool.lock().unwrap().conflicts, 0);
    }

    /// The leaseless convergence path is still correct (it is the safety
    /// net for backends without `If-Match`): two writers create the same
    /// name, and the record later in the global log wins on every
    /// replica. Here the lease is bypassed deliberately.
    #[tokio::test]
    async fn same_name_conflict_converges_last_wins() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let fa = a.meta.create(1, "x", 0o644, 0, 0).unwrap();
        let fb = b.meta.create(1, "x", 0o644, 0, 0).unwrap();
        assert_ne!(fa.ino, fb.ino);

        a.sync().await; // A's create is log seq 1
        a.release().await;
        b.sync().await; // B tails A (conflict: B's pending wins), ships at 2
        a.sync().await; // A tails B: last-wins -> B's ino

        let ia = a.meta.lookup(1, "x").unwrap().unwrap().ino;
        let ib = b.meta.lookup(1, "x").unwrap().unwrap().ino;
        assert_eq!(ia, ib, "replicas must agree");
        assert_eq!(ia, fb.ino, "the later log record wins");
        assert_eq!(b.ship.spool.lock().unwrap().conflicts, 1);
    }

    /// Deep sequential workflow: A builds a tree and publishes; B
    /// tails, mutates, publishes; A tails. Replicas stay identical.
    #[tokio::test]
    async fn sequential_cross_node_edits_converge() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let d = a.meta.mkdir(1, "proj", 0o755, 0, 0).unwrap();
        let f = a.meta.create(d.ino, "main.rs", 0o644, 0, 0).unwrap();
        a.meta.set_manifest(f.ino, b"v1", 2).unwrap();
        a.sync().await;
        a.release().await;
        b.sync().await;

        // B sees A's tree, edits and renames.
        let bd = b.meta.lookup(1, "proj").unwrap().unwrap();
        let bf = b.meta.lookup(bd.ino, "main.rs").unwrap().unwrap();
        assert_eq!(b.meta.manifest(bf.ino).unwrap().unwrap(), b"v1");
        b.meta.set_manifest(bf.ino, b"v2-longer", 9).unwrap();
        b.meta.rename(bd.ino, "main.rs", bd.ino, "lib.rs").unwrap();
        b.sync().await;
        a.sync().await;

        let af = a.meta.lookup(d.ino, "lib.rs").unwrap().unwrap();
        assert_eq!(a.meta.manifest(af.ino).unwrap().unwrap(), b"v2-longer");
        assert!(a.meta.lookup(d.ino, "main.rs").unwrap().is_none());
        assert_eq!(af.size, 9);
        assert_eq!(a.ship.spool.lock().unwrap().conflicts, 0);
        assert_eq!(b.ship.spool.lock().unwrap().conflicts, 0);
    }

    /// A node that crashed after PUT but before ack recovers by
    /// recognizing its own segment at the head of the log.
    #[tokio::test]
    async fn own_segment_recovery_after_lost_response() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();

        // Simulate the lost response: the segment lands in S3 but the
        // journal was never acked and next_seq never advanced.
        let records: Vec<LogRecord> = a
            .meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        a.ship
            .log
            .put_segment(1, &encode(1, 1, &records).unwrap())
            .await
            .unwrap();

        a.sync().await;
        assert_eq!(a.meta.journal_len().unwrap(), 0, "journal acked");
        assert_eq!(a.ship.next_seq, 2);
        // The op is not applied twice (dir still exists exactly once).
        assert_eq!(names(&a.meta, 1).len(), 1);
    }

    /// Nothing ships without authority, and what ships carries the
    /// holder's epoch.
    #[tokio::test]
    async fn ship_requires_the_lease_and_stamps_its_epoch() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();

        // Leaseless sync: the journal must stay put.
        a.ship.sync(&a.lease).await.unwrap();
        assert_eq!(a.meta.journal_len().unwrap(), 1, "shipped without a lease");
        assert!(a.ship.log.list_segments().await.unwrap().is_empty());

        a.sync().await;
        assert_eq!(a.meta.journal_len().unwrap(), 0);
        let seg = segment(&store, 1).await;
        assert_eq!((seg.node, seg.epoch), (1, 1));
    }

    /// B takes A's expired lease. The takeover must apply A's flushed
    /// log first — B's replica shows A's tree before B writes anything.
    #[tokio::test]
    async fn takeover_applies_the_predecessors_log_first() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert!(b.meta.lookup(1, "from-a").unwrap().is_none());

        // A vanishes without releasing; its lease expires.
        expire_lease(&store).await;
        assert!(acquire_lease(&mut b.ship, &mut b.lease).await.unwrap());
        assert!(
            b.meta.lookup(1, "from-a").unwrap().is_some(),
            "takeover must not precede applying the old holder's log"
        );
        assert_eq!(b.lease.ship_epoch(), Some(2));
    }

    /// Committing a takeover without the tail witness is refused: the
    /// ordering rule is enforced by the type, not by convention.
    #[tokio::test]
    async fn takeover_without_tail_witness_is_refused() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        expire_lease(&store).await;

        let plan = b.lease.classify().await.unwrap();
        assert!(plan.needs_tail());
        let err = b.lease.commit(plan, None).await.unwrap_err();
        assert!(err.to_string().contains("without applying its flushed log"));
    }

    /// A deposed holder refuses to ship and keeps its journal intact;
    /// the fence rejects the late segment if it ever lands.
    #[tokio::test]
    async fn deposed_holder_refuses_to_ship() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        // A stalls (frozen process) and its lease expires; B takes over.
        expire_lease(&store).await;
        b.sync().await;
        b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        b.sync().await;

        // A wakes up with unshipped records and tries to continue.
        a.meta.mkdir(1, "stranded", 0o755, 0, 0).unwrap();
        a.lease.renew_now().await.unwrap();
        assert!(a.lease.is_lost(), "A must notice it was deposed");
        assert_eq!(a.lease.ship_epoch(), None);

        a.ship.sync(&a.lease).await.unwrap();
        assert_eq!(
            a.meta.journal_len().unwrap(),
            1,
            "stranded writes must stay in the journal, not vanish or ship"
        );
        // And it never reacquires: deposition is terminal until a
        // phase-4 reintegration path exists.
        assert!(acquire_lease(&mut a.ship, &mut a.lease).await.is_err());
        // B's namespace is intact.
        assert_eq!(names(&b.meta, 1).len(), 2);
        assert!(b.meta.lookup(1, "stranded").unwrap().is_none());
    }

    /// A late segment stamped with a superseded epoch is fenced out
    /// rather than applied over the current holder's state.
    #[tokio::test]
    async fn lower_epoch_segment_is_fenced() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        expire_lease(&store).await;
        b.sync().await; // epoch 2
        b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        b.sync().await;

        // Forge a deposed holder's flush at the next free sequence.
        let stranded = vec![LogRecord::Mkdir {
            parent: 1,
            name: "zombie".into(),
            ino: 1 << 40 | 99,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 0,
        }];
        let seq = b.ship.next_seq;
        b.ship
            .log
            .put_segment(seq, &encode(1, 1, &stranded).unwrap())
            .await
            .unwrap();

        b.sync().await;
        assert!(
            b.meta.lookup(1, "zombie").unwrap().is_none(),
            "a superseded epoch must not mutate the namespace"
        );
        assert_eq!(b.ship.spool.lock().unwrap().fenced, 1);
        assert_eq!(b.meta.applied_seq().unwrap(), seq);
    }

    /// Rewrite the lease object so it is already expired, simulating a
    /// holder that stopped renewing.
    async fn expire_lease(store: &StdArc<InMemory>) {
        use constellation_store_s3::lease::Lease;
        let ls = LeaseStore::new(
            store.clone(),
            constellation_store_s3::log::PARTITION,
            LeaseMode::Cas,
        );
        let (cur, tag) = ls.get().await.unwrap().unwrap();
        let expired = Lease {
            expires_unix_ms: 1,
            ..cur
        };
        ls.try_swap(&expired, &tag).await.unwrap();
    }
}
