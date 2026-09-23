//! Deposition recovery on demand (the `reintegrate` control command).
//!
//! Plan 30 §M3b replaced reintegration — classifying a deposed holder's
//! stranded journal against a side replica and re-journaling the "clean"
//! records — with rollback plus replay by rid
//! (`recovery::recover_deposed`): the unshipped transactions are rolled
//! back from their captured before-images and re-executed, exactly once,
//! through whoever holds the lease now. That runs by itself on the next
//! sync round after a node learns it was deposed; this module keeps the
//! operator-facing command, which runs it at once and reports what it
//! did, and the `status.reintegration` block.

use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::lease::LeaseKeeper;
use crate::shipper::Shipper;

#[derive(Default)]
pub struct ReintegrationState {
    pub in_progress: AtomicBool,
}

impl ReintegrationState {
    /// `stranded`: journal rows still waiting on a deposition recovery;
    /// `conflicts`: refused replays materialized as conflict copies.
    pub fn snapshot(
        &self,
        stranded: u64,
        conflicts: u64,
    ) -> constellation_api::ReintegrationStatus {
        constellation_api::ReintegrationStatus {
            stranded_records: stranded,
            conflicts_materialized: conflicts,
            in_progress: self.in_progress.load(Ordering::Relaxed),
        }
    }
}

/// Run the deposition recovery for every partition this node was deposed
/// from (its keeper is lost, or a deposition was persisted before a
/// restart). A node that was not deposed has nothing to recover.
pub async fn run(
    ship: &mut Shipper,
    keepers: &mut HashMap<String, LeaseKeeper>,
    state_dir: &std::path::Path,
    flags: &ReintegrationState,
) -> Result<String> {
    flags.in_progress.store(true, Ordering::Relaxed);
    let _guard = InProgressGuard(flags);
    let persisted = matches!(ship.meta().kv_get("lease_lost")?.as_deref(), Some("1"));
    let mut summaries = Vec::new();
    for keeper in keepers.values_mut() {
        if !keeper.is_lost() && !persisted {
            continue;
        }
        if !keeper.is_lost() {
            keeper.force_lost();
        }
        summaries.push(crate::recovery::recover_deposed(ship, keeper, Some(state_dir)).await?);
    }
    if summaries.is_empty() {
        return Ok("not deposed: nothing to recover".into());
    }
    Ok(summaries.join("; "))
}

struct InProgressGuard<'a>(&'a ReintegrationState);
impl Drop for InProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.in_progress.store(false, Ordering::Relaxed);
    }
}
