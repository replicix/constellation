//! Deposition recovery on demand (the `reintegrate` control command).
//!
//! Plan 30 §M3b replaced reintegration — classifying a deposed holder's
//! stranded journal against a side replica and re-journaling the "clean"
//! records — with rollback plus replay by rid: the unshipped transactions
//! are rolled back from their captured before-images and re-executed,
//! exactly once, through whoever holds the lease now. Plan 30 M5 moved
//! that recovery into the authority core (`Control::Reintegrate` runs it
//! at once; every round runs it by itself after a deposition). This
//! module keeps the `status.reintegration` block.

use std::sync::atomic::{AtomicBool, Ordering};

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
