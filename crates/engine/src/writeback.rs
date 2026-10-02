//! Streaming-write and write-back policy (plan 08).
//!
//! The data plane has one durable queue: the metadata store's (fjall)
//! `pending_upload` partition. Write-through waits for that queue (scoped
//! to the inode when possible); write-back journals locally and lets the
//! existing sync task drain it before shipping metadata. There is
//! deliberately no second background-upload path: continuation epochs,
//! normal write-back, handoff, leave, and unmount all meet at the same
//! drain.
//!
//! `fsync`, `fdatasync`, `O_SYNC`, and `--fsync-mode s3` conservatively
//! force write-through. A local-only fsync could satisfy POSIX and
//! survive process crash/reboot, but acknowledging bytes that still
//! disappear with permanent node loss is surprising. Applications
//! explicitly asking for a barrier therefore keep the stronger rule.
//!
//! **The rule is the file's, not the call's** (plan 39b). Linux `fsync(2)`
//! makes "all modified in-core data of the file" durable, whichever
//! descriptor or process wrote it — a PostgreSQL checkpointer `fsync`s
//! files its backends wrote and closed. So an `fsync` (and `fdatasync`,
//! and an `O_SYNC`/`O_DSYNC` write) drains every `pending_upload` row of
//! the inode, in both `--fsync-mode`s (`View::durable_run`): the chunks an
//! earlier `close()` under `back` queued, a failed write-through close's,
//! a failed earlier `fsync`'s. `--fsync-mode local` then means "this
//! file's chunks in S3 and this node's metadata store synced" (plus what
//! the ack policy gives); `s3` additionally ships the journal.
//!
//! Before 39b this was asymmetric. `flush_inode` only drains a write
//! session it publishes, so under `local` an `open` + `fsync` of a file
//! closed under `back` found no session and returned with the chunks
//! still only on this node; only `s3`'s barrier drained them. And inside
//! a continuation epoch (plan 30 §M10) a `local` `fsync` skipped the drain
//! altogether — including the retry of one started before the epoch,
//! which then succeeded once the epoch activated. Both are gone: the
//! epoch exempts a `close()` (`finish_flush` still skips its drain there),
//! never a barrier, which waits for the bucket — or a peer's handoff —
//! like an NFS `hard` mount, bounded only by `--fsync-timeout` (plan 39).
//! The per-view, in-memory "owed" set plan 39 added for a failed `fsync`
//! is gone with it: the durable table is the source of truth, survives a
//! restart, and covers every way a chunk can be left behind. An inode
//! with nothing pending costs one seek in the table's by-inode mirror
//! (`pending_upload_by_ino`), no sync-task round trip.
//!
//! Never a false success: a queued chunk of the file that is in neither
//! this node's cache nor S3 (a torn disk) fails the `fsync` `EIO`, now
//! and every time after while its row stays; and on the node that
//! sequenced another node's `back` close, the rows that close forwarded
//! as pending (`meta::store::remote`) are waited for until the writer's
//! upload lands — the `fsync` covers the file's chunks queued anywhere as
//! this node sees them.
//!
//! Dirty pressure follows DESIGN.md §9: clean eviction happens inside
//! `DiskCache::insert`, then writes are delayed increasingly from 75%
//! of budget, and only the hard limit returns ENOSPC. The delay is
//! intentionally bounded so a healed uploader can make progress.

use std::str::FromStr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Through = 0,
    Back = 1,
}

impl WriteMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Through => "through",
            Self::Back => "back",
        }
    }
}

impl FromStr for WriteMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "through" => Ok(Self::Through),
            "back" => Ok(Self::Back),
            _ => Err("expected through or back"),
        }
    }
}

/// The mount's write mode, and plan 31 C8's upload hold
/// (`UploadMode::UnmeteredOnly` on a metered network), which makes every
/// close that did not ask for durability behave as `back`: the close
/// journals locally and leaves its chunks to the (held) background pass,
/// instead of waiting for an upload the hold would never start.
pub struct WriteModeState(AtomicU8, std::sync::atomic::AtomicBool);

impl WriteModeState {
    pub fn new(mode: WriteMode) -> Self {
        Self(
            AtomicU8::new(mode as u8),
            std::sync::atomic::AtomicBool::new(false),
        )
    }

    /// Plan 31 C8: uploads are held (see the type's docs).
    pub fn set_upload_hold(&self, held: bool) {
        self.1.store(held, Ordering::Relaxed);
    }

    pub fn upload_hold(&self) -> bool {
        self.1.load(Ordering::Relaxed)
    }

    pub fn get(&self) -> WriteMode {
        if self.0.load(Ordering::Relaxed) == WriteMode::Back as u8 {
            WriteMode::Back
        } else {
            WriteMode::Through
        }
    }

    pub fn set(&self, mode: WriteMode) {
        self.0.store(mode as u8, Ordering::Relaxed);
    }

    pub fn effective(&self, explicit_sync: bool, open_sync: bool, fsync_s3: bool) -> WriteMode {
        if explicit_sync || open_sync || fsync_s3 {
            WriteMode::Through
        } else if self.upload_hold() {
            WriteMode::Back
        } else {
            self.get()
        }
    }
}

/// Delay before accepting another dirty byte at this occupancy.
// `Err(())` is "over budget: refuse"; public since plan 31 C3 (the FUSE
// adapter throttles on it), which is what makes clippy ask for an error type.
#[allow(clippy::result_unit_err)]
pub fn throttle_delay(used: u64, budget: u64) -> Result<Duration, ()> {
    if budget == 0 || used >= budget {
        return Err(());
    }
    let ratio = used as f64 / budget as f64;
    if ratio < 0.75 {
        return Ok(Duration::ZERO);
    }
    let pressure = ((ratio - 0.75) / 0.25).clamp(0.0, 1.0);
    Ok(Duration::from_millis((pressure * pressure * 250.0) as u64))
}

/// EWMA existence-probe policy. A probe is worth its extra RTT only
/// above the approximate 8% request-price break-even. Separate
/// enable/disable thresholds provide hysteresis.
#[derive(Debug)]
pub struct ProbePolicy {
    hit_rate: f64,
    samples: u64,
    enabled: bool,
}

impl Default for ProbePolicy {
    fn default() -> Self {
        Self {
            hit_rate: 0.5,
            samples: 0,
            enabled: true,
        }
    }
}

impl ProbePolicy {
    const ALPHA: f64 = 0.25;
    const DISABLE_BELOW: f64 = 0.05;
    const ENABLE_ABOVE: f64 = 0.15;

    pub fn record(&mut self, hit: bool) {
        let sample = if hit { 1.0 } else { 0.0 };
        if self.samples == 0 {
            self.hit_rate = sample;
        } else {
            self.hit_rate = Self::ALPHA * sample + (1.0 - Self::ALPHA) * self.hit_rate;
        }
        self.samples += 1;
        if self.enabled && self.samples >= 4 && self.hit_rate < Self::DISABLE_BELOW {
            self.enabled = false;
        } else if !self.enabled && self.hit_rate > Self::ENABLE_ABOVE {
            self.enabled = true;
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn hit_rate(&self) -> f64 {
        self.hit_rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_escapes_force_through() {
        let state = WriteModeState::new(WriteMode::Back);
        assert_eq!(state.effective(false, false, false), WriteMode::Back);
        assert_eq!(state.effective(true, false, false), WriteMode::Through);
        assert_eq!(state.effective(false, true, false), WriteMode::Through);
        assert_eq!(state.effective(false, false, true), WriteMode::Through);
        state.set(WriteMode::Through);
        assert_eq!(state.effective(false, false, false), WriteMode::Through);
    }

    #[test]
    fn the_upload_hold_makes_plain_closes_write_back_but_never_a_sync() {
        let state = WriteModeState::new(WriteMode::Through);
        state.set_upload_hold(true);
        assert_eq!(state.effective(false, false, false), WriteMode::Back);
        assert_eq!(state.effective(true, false, false), WriteMode::Through);
        assert_eq!(state.effective(false, true, false), WriteMode::Through);
        assert_eq!(state.effective(false, false, true), WriteMode::Through);
        assert_eq!(state.get(), WriteMode::Through, "the configured mode stays");
        state.set_upload_hold(false);
        assert_eq!(state.effective(false, false, false), WriteMode::Through);
    }

    #[test]
    fn throttle_precedes_hard_enospc() {
        let budget = 1000;
        assert_eq!(throttle_delay(500, budget).unwrap(), Duration::ZERO);
        let first = throttle_delay(800, budget).unwrap();
        let later = throttle_delay(950, budget).unwrap();
        assert!(first > Duration::ZERO);
        assert!(later > first);
        assert!(throttle_delay(999, budget).is_ok());
        assert!(throttle_delay(1000, budget).is_err());
    }

    #[test]
    fn probe_hysteresis_does_not_flap() {
        let mut p = ProbePolicy::default();
        for _ in 0..16 {
            p.record(false);
        }
        assert!(!p.enabled());
        p.record(true);
        assert!(
            p.enabled(),
            "a sustained EWMA rise crosses the upper threshold"
        );
        p.record(false);
        assert!(p.enabled(), "one miss cannot immediately disable probing");
    }
}
