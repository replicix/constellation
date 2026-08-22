//! Metadata log shipping (DESIGN.md §4): drains the local journal into
//! CAS-created S3 log segments, writes periodic checkpoints, and
//! bootstraps fresh nodes from checkpoint + segment replay.

use anyhow::{bail, Context, Result};
use constellation_meta::{LogRecord, MetaStore, SqliteMeta};
use constellation_store_s3::LogStore;
use std::sync::Arc;

/// Checkpoint after this many shipped segments.
const CHECKPOINT_EVERY: u64 = 32;
/// Max journal records per segment.
const SEGMENT_BATCH: usize = 10_000;

pub struct Shipper {
    meta: Arc<SqliteMeta>,
    log: LogStore,
    next_seq: u64,
    shipped_since_ckpt: u64,
}

fn encode(records: &[LogRecord]) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(records)?)
}

fn decode(payload: &[u8]) -> Result<Vec<LogRecord>> {
    Ok(serde_json::from_slice(payload)?)
}

impl Shipper {
    /// Attach to an existing local replica: find the next sequence
    /// number and recover from a crash between segment PUT and journal
    /// ack (the shipped-but-unacked batch is re-acked, not re-shipped).
    pub async fn attach(meta: Arc<SqliteMeta>, log: LogStore) -> Result<Self> {
        let segments = log.list_segments().await?;
        let last_seq = segments.last().copied().unwrap_or(0);
        let mut shipper = Self {
            meta,
            log,
            next_seq: last_seq + 1,
            shipped_since_ckpt: 0,
        };
        if last_seq > 0 {
            shipper.recover_unacked(last_seq).await?;
        }
        Ok(shipper)
    }

    /// If the head of the journal is byte-identical to the last shipped
    /// segment, the previous process crashed after PUT but before ack.
    async fn recover_unacked(&mut self, last_seq: u64) -> Result<()> {
        let journal = self.meta.take_journal(SEGMENT_BATCH)?;
        if journal.is_empty() {
            return Ok(());
        }
        let last = decode(&self.log.get_segment(last_seq).await?)?;
        if journal.len() >= last.len()
            && journal
                .iter()
                .take(last.len())
                .map(|(_, r)| r)
                .eq(last.iter())
        {
            let upto = journal[last.len() - 1].0;
            self.meta.ack_journal(upto)?;
            tracing::info!(
                seq = last_seq,
                records = last.len(),
                "recovered unacked segment"
            );
        }
        Ok(())
    }

    /// Ship one batch if the journal is non-empty. Returns whether a
    /// segment was written.
    pub async fn flush(&mut self) -> Result<bool> {
        let batch = self.meta.take_journal(SEGMENT_BATCH)?;
        let Some((last_journal_seq, _)) = batch.last() else {
            return Ok(false);
        };
        let last_journal_seq = *last_journal_seq;
        let records: Vec<LogRecord> = batch.iter().map(|(_, r)| r.clone()).collect();
        let payload = encode(&records)?;
        match self.log.put_segment(self.next_seq, &payload).await {
            Ok(()) => {}
            Err(constellation_store_s3::StoreError::AlreadyExists) => {
                // Our own crash-replay wrote it, or a second writer is
                // active. Identical content is benign; anything else is
                // fatal in single-writer phase 1.
                let existing = self.log.get_segment(self.next_seq).await?;
                if existing != payload {
                    bail!(
                        "log segment {} already exists with different content: \
                         another writer is active on this filesystem",
                        self.next_seq
                    );
                }
            }
            Err(e) => return Err(e).context("shipping log segment"),
        }
        self.meta.ack_journal(last_journal_seq)?;
        tracing::debug!(
            seq = self.next_seq,
            records = records.len(),
            "shipped log segment"
        );
        self.next_seq += 1;
        self.shipped_since_ckpt += 1;
        if self.shipped_since_ckpt >= CHECKPOINT_EVERY {
            self.checkpoint().await?;
        }
        Ok(true)
    }

    /// Snapshot the local DB as a checkpoint covering the shipped log.
    pub async fn checkpoint(&mut self) -> Result<()> {
        if self.next_seq == 1 {
            return Ok(()); // nothing shipped, nothing to cover
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

    /// Final flush + checkpoint on clean unmount.
    pub async fn shutdown(&mut self) -> Result<()> {
        while self.flush().await? {}
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
    for seq in log.list_segments().await? {
        if seq <= from_seq {
            continue;
        }
        let records = decode(&log.get_segment(seq).await?)?;
        replayed += records.len();
        meta.apply_records(&records)
            .with_context(|| format!("replaying log segment {seq}"))?;
    }
    // No process holds unlinked-but-open files on a brand-new replica.
    for ino in meta.orphans()? {
        meta.reap_orphan(ino)?;
    }
    tracing::info!(from_seq, replayed, "bootstrapped metadata replica");
    Ok(())
}
