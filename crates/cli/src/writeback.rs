//! Streaming-write and write-back policy (plan 05b).
//!
//! The data plane has one durable queue: SQLite's `pending_upload`
//! table. Write-through waits for that queue (scoped to the inode when
//! possible); write-back journals locally and lets the existing sync
//! task drain it before shipping metadata. There is deliberately no
//! second background-upload path: continuation epochs, normal
//! write-back, handoff, leave, and unmount all meet at the same drain.
//!
//! `fsync`, `fdatasync`, `O_SYNC`, and `--fsync-mode s3` conservatively
//! force write-through. A local-only fsync could satisfy POSIX and
//! survive process crash/reboot, but acknowledging bytes that still
//! disappear with permanent node loss is surprising. Applications
//! explicitly asking for a barrier therefore keep the stronger rule.
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

pub struct WriteModeState(AtomicU8);

impl WriteModeState {
    pub fn new(mode: WriteMode) -> Self {
        Self(AtomicU8::new(mode as u8))
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
        } else {
            self.get()
        }
    }
}

/// Delay before accepting another dirty byte at this occupancy.
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
