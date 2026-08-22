//! Metadata log sync (DESIGN.md §4): one component both **tails** the
//! shared S3 log (applying other nodes' segments to the local replica)
//! and **ships** the local journal as CAS-created segments. The CAS
//! collision on a sequence number is the multi-writer conflict
//! detector: the loser applies the winner's segment and retries at the
//! next sequence (DESIGN.md "losers re-tail and retry").
//!
//! Convergence without leases (phase 2): tailed foreign records apply
//! with last-wins semantics *except* where they touch state that
//! pending (unshipped) local records touch — those are skipped, because
//! our records will sit later in the global log and win on every other
//! replica too. Leases (phase 3) will make such conflicts impossible in
//! the default mode; here they are detected and logged.

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

/// What one log segment holds: its origin node and a batch of records.
/// Versioned; old bare-array segments decode as node 0.
#[derive(Serialize, Deserialize)]
struct SegmentEnvelope {
    v: u32,
    node: u64,
    records: Vec<LogRecord>,
}

fn encode(node: u64, records: &[LogRecord]) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&SegmentEnvelope {
        v: 1,
        node,
        records: records.to_vec(),
    })?)
}

fn decode(payload: &[u8]) -> Result<(u64, Vec<LogRecord>)> {
    if let Ok(env) = serde_json::from_slice::<SegmentEnvelope>(payload) {
        return Ok((env.node, env.records));
    }
    // Pre-envelope segments: a bare JSON array from the genesis node.
    let records: Vec<LogRecord> = serde_json::from_slice(payload)?;
    Ok((0, records))
}

pub struct Shipper {
    meta: Arc<SqliteMeta>,
    log: LogStore,
    node_id: u64,
    /// Next sequence number not yet seen (neither shipped nor tailed).
    next_seq: u64,
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
            shipped_since_ckpt: 0,
            spool: Arc::new(std::sync::Mutex::new(SpoolInfo {
                head_seq: applied,
                conflicts: 0,
                last_error: None,
            })),
        })
    }

    /// One full sync round: tail new segments, then ship the journal
    /// until it drains (re-tailing after every CAS collision).
    pub async fn sync(&mut self) -> Result<()> {
        loop {
            self.tail().await?;
            if !self.ship_one().await? {
                return Ok(());
            }
        }
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
        let (node, records) = decode(&self.log.get_segment(seq).await?)?;
        if node == self.node_id {
            // Our own segment from a previous life: the PUT succeeded
            // but the response (or the ack) was lost. The journal head
            // must match; ack it instead of re-applying.
            let journal = self.meta.take_journal(records.len())?;
            let matches =
                journal.len() == records.len() && journal.iter().map(|(_, r)| r).eq(records.iter());
            if !matches {
                bail!(
                    "segment {seq} claims our node id {} but does not match \
                     the journal head: state dir reuse or id collision",
                    self.node_id
                );
            }
            self.meta.ack_journal_at(journal.last().unwrap().0, seq)?;
            tracing::info!(seq, records = records.len(), "recovered unacked segment");
        } else {
            let pending =
                TouchSet::from_records(self.meta.take_journal(usize::MAX)?.iter().map(|(_, r)| r));
            let skipped = self.meta.apply_foreign(&records, &pending)?;
            if skipped > 0 {
                self.spool.lock().unwrap().conflicts += skipped as u64;
            }
            tracing::debug!(
                seq,
                node,
                records = records.len(),
                skipped,
                "applied foreign segment"
            );
            self.meta.set_applied_seq(seq)?;
        }
        self.advance(seq);
        Ok(())
    }

    /// Ship one journal batch at `next_seq`. Returns true if another
    /// round is needed (more records pending, or a CAS collision).
    async fn ship_one(&mut self) -> Result<bool> {
        let batch = self.meta.take_journal(SEGMENT_BATCH)?;
        if batch.is_empty() {
            return Ok(false);
        }
        let records: Vec<LogRecord> = batch.iter().map(|(_, r)| r.clone()).collect();
        let payload = encode(self.node_id, &records)?;
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
            records = records.len(),
            "shipped log segment"
        );
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
    pub async fn shutdown(&mut self) -> Result<()> {
        self.sync().await?;
        if self.shipped_since_ckpt > 0 {
            self.checkpoint().await?;
        }
        Ok(())
    }
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
        let (_, records) = decode(&log.get_segment(seq).await?)?;
        replayed += records.len();
        meta.apply_records(&records)
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
    use constellation_meta::MetaStore;
    use object_store::memory::InMemory;
    use std::sync::Arc as StdArc;

    struct Node {
        meta: Arc<SqliteMeta>,
        ship: Shipper,
    }

    fn node(store: &StdArc<InMemory>, id: u64) -> Node {
        let meta = Arc::new(SqliteMeta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let log = LogStore::new(store.clone());
        let ship = Shipper::attach(meta.clone(), log, id).unwrap();
        Node { meta, ship }
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

    /// Two writers on disjoint names: both replicas converge to the
    /// union, and ino prefixes never collide.
    #[tokio::test]
    async fn two_nodes_disjoint_converge() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let da = a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        let db = b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        assert_ne!(da.ino >> 40, db.ino >> 40, "ino prefixes must differ");

        a.ship.sync().await.unwrap(); // ships A's segment
        b.ship.sync().await.unwrap(); // CAS collision -> tails A, ships B
        a.ship.sync().await.unwrap(); // tails B

        assert_eq!(names(&a.meta, 1), names(&b.meta, 1));
        assert_eq!(names(&a.meta, 1).len(), 2);
    }

    /// Two writers create the same name concurrently: the record later
    /// in the global log wins on every replica (here B, which shipped
    /// after A). Detected as a conflict, never silent divergence.
    #[tokio::test]
    async fn same_name_conflict_converges_last_wins() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let fa = a.meta.create(1, "x", 0o644, 0, 0).unwrap();
        let fb = b.meta.create(1, "x", 0o644, 0, 0).unwrap();
        assert_ne!(fa.ino, fb.ino);

        a.ship.sync().await.unwrap(); // A's create is log seq 1
        b.ship.sync().await.unwrap(); // B tails A (conflict: B's pending wins), ships at 2
        a.ship.sync().await.unwrap(); // A tails B: last-wins -> B's ino

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
        a.ship.sync().await.unwrap();
        b.ship.sync().await.unwrap();

        // B sees A's tree, edits and renames.
        let bd = b.meta.lookup(1, "proj").unwrap().unwrap();
        let bf = b.meta.lookup(bd.ino, "main.rs").unwrap().unwrap();
        assert_eq!(b.meta.manifest(bf.ino).unwrap().unwrap(), b"v1");
        b.meta.set_manifest(bf.ino, b"v2-longer", 9).unwrap();
        b.meta.rename(bd.ino, "main.rs", bd.ino, "lib.rs").unwrap();
        b.ship.sync().await.unwrap();
        a.ship.sync().await.unwrap();

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
            .put_segment(1, &encode(1, &records).unwrap())
            .await
            .unwrap();

        a.ship.sync().await.unwrap();
        assert_eq!(a.meta.journal_len().unwrap(), 0, "journal acked");
        assert_eq!(a.ship.next_seq, 2);
        // The op is not applied twice (dir still exists exactly once).
        assert_eq!(names(&a.meta, 1).len(), 1);
    }
}
