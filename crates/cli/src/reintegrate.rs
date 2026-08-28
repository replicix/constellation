//! Stranded-journal reintegration for the mount daemon.
//!
//! Classifies each unmarked journal record against a side replica
//! bootstrapped from the shared log, then either re-journals (clean) or
//! materializes a conflict file. After success the `lost` flag is
//! cleared so the node may acquire leases again.

use anyhow::{Context, Result};
use constellation_meta::{classify, materialize, Disposition, MetaStore, SqliteMeta};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::lease::LeaseKeeper;
use crate::shipper::{self, Shipper};

pub struct ReintegrationState {
    pub in_progress: AtomicBool,
    pub conflicts: AtomicU64,
}

impl Default for ReintegrationState {
    fn default() -> Self {
        Self {
            in_progress: AtomicBool::new(false),
            conflicts: AtomicU64::new(0),
        }
    }
}

impl ReintegrationState {
    pub fn snapshot(&self, stranded: u64) -> constellation_api::ReintegrationStatus {
        constellation_api::ReintegrationStatus {
            stranded_records: stranded,
            conflicts_materialized: self.conflicts.load(Ordering::Relaxed),
            in_progress: self.in_progress.load(Ordering::Relaxed),
        }
    }
}

pub async fn run(
    meta: &SqliteMeta,
    ship: &mut Shipper,
    keepers: &mut HashMap<String, LeaseKeeper>,
    node_id: u64,
    state_dir: &std::path::Path,
    flags: &ReintegrationState,
) -> Result<String> {
    flags.in_progress.store(true, Ordering::Relaxed);
    let _guard = InProgressGuard(flags);
    flags.conflicts.store(
        meta.reintegration_conflict_count().unwrap_or(0),
        Ordering::Relaxed,
    );

    let result = async {
        // Need write leases: reintegration appends records like any writer.
        for part in meta.unmarked_journal_parts()? {
            keepers
                .entry(part.clone())
                .or_insert_with(|| ship.lease_keeper(&part));
        }
        for (part, keeper) in keepers.iter_mut() {
            // The persisted lost bit remains set until the whole procedure
            // succeeds; this temporary in-memory unlock only permits the
            // explicit reintegration acquisition path.
            keeper.clear_lost();
            if !shipper::acquire_lease_for(ship, keeper, part).await? {
                anyhow::bail!("could not acquire write lease for {part}; retry reintegrate later");
            }
        }

        let view_path = state_dir.join(".reintegrate-view.db");
        let _ = std::fs::remove_file(&view_path);
        shipper::bootstrap(&view_path, ship.log())
            .await
            .context("bootstrapping shared-log view for reintegration")?;
        let shared = SqliteMeta::open(&view_path)?;

        let stranded = meta.unmarked_journal()?;
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut cleaned = 0u64;
        let mut conflicts = 0u64;
        let mut dispositions = Vec::new();
        let mut output = Vec::new();
        for (seq, rec) in stranded {
            let disp = classify(&shared, &rec)?;
            match &disp {
                Disposition::Clean => {
                    // Advance the reconciled namespace in journal order;
                    // the same record is appended under a fresh local seq.
                    shared.apply_records(std::slice::from_ref(&rec))?;
                    output.push(rec);
                    cleaned += 1;
                }
                Disposition::Conflict { reason } => {
                    let path = materialize(&shared, &rec, node_id, ts)?;
                    tracing::error!(
                        seq,
                        path,
                        reason,
                        "reintegration conflict: stranded version materialized"
                    );
                    conflicts += 1;
                    flags.conflicts.fetch_add(1, Ordering::Relaxed);
                }
            }
            dispositions.push((seq, disp.as_str().to_string(), disp.detail()));
        }
        // Materialization used ordinary namespace operations on the side
        // replica; append those generated records too.
        output.extend(
            shared
                .take_journal(usize::MAX)?
                .into_iter()
                .map(|(_, record)| record),
        );
        meta.commit_reintegration_batch(&view_path, &dispositions, &output)?;
        drop(shared);
        let _ = std::fs::remove_file(&view_path);

        ship.sync_all_for_reintegration(keepers)
            .await
            .context("shipping reintegrated journal")?;

        for keeper in keepers.values_mut() {
            keeper.clear_lost();
        }
        meta.kv_set("lease_lost", "0")?;

        Ok(format!(
            "reintegrated: {cleaned} clean, {conflicts} conflict(s) materialized"
        ))
    }
    .await;

    if result.is_err() {
        for keeper in keepers.values_mut() {
            keeper.force_lost();
        }
    }
    result
}

struct InProgressGuard<'a>(&'a ReintegrationState);
impl Drop for InProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.in_progress.store(false, Ordering::Relaxed);
    }
}
