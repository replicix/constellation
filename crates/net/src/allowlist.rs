//! Accept-time allowlist (DESIGN.md §8).
//!
//! A peer connection is accepted only when its public key appears in the
//! filesystem's node registry. Enrolling a key means writing
//! `nodes/<id>.json`, which needs bucket write permission, so **IAM
//! stays the trust root** — possessing a key is not enough.
//!
//! On a miss the registry is refreshed once before rejecting, because a
//! legitimately new peer may have enrolled since the last refresh. That
//! refresh is rate-limited so an unknown key cannot turn into an S3
//! request amplifier.
//!
//! Only *miss-triggered* refreshes arm that rate limit. The periodic
//! registry refresh must not: on a cold start every node re-reads the
//! registry the moment it comes up, which is exactly when its peers are
//! still enrolling. If that startup read armed the cooldown, a peer
//! that enrolled milliseconds later would be rejected for the whole
//! cooldown window — its forwarded mutations would fail as transport
//! errors and escalate into a lease takeover the moment two mounts
//! start together (observed as `forwarded-mutations` flaking).
//!
//! Nor does a miss-triggered refresh that *found* the key it was for:
//! that miss was a legitimate new peer, not an unknown key probing, so
//! it spends no cooldown. Otherwise the first peer to join after us
//! locks out every peer joining within the next five seconds — with
//! three mounts starting together the third one's forwards to the holder
//! were rejected until the holder's next periodic registry read (harness
//! `session-ryw-after-holder-kill` and `takeover-marker-strands-promptly`,
//! whose third node then fell back to the S3 inbox). Only a futile
//! refresh — the key still unknown after it — arms the cooldown, which is
//! all the amplification guard needs: an unenrolled key can never make
//! its own refresh productive.

use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Minimum spacing between registry refreshes triggered by a miss.
const REFRESH_COOLDOWN: Duration = Duration::from_secs(5);

/// The set of pubkeys currently permitted to connect.
pub struct Allowlist {
    keys: HashSet<String>,
    /// When a miss last triggered a refresh; periodic refreshes via
    /// [`Allowlist::replace`] never arm it (one that enrols the key whose
    /// miss armed it gives the arming back).
    last_miss_refresh: Option<Instant>,
    /// The key whose miss armed `last_miss_refresh` (with the arming it
    /// replaced): a refresh that turns out to enrol it disarms again.
    armed_by: Option<(String, Option<Instant>)>,
    cooldown: Duration,
}

impl Default for Allowlist {
    fn default() -> Self {
        Self::new()
    }
}

impl Allowlist {
    pub fn new() -> Self {
        Self {
            keys: HashSet::new(),
            last_miss_refresh: None,
            armed_by: None,
            cooldown: REFRESH_COOLDOWN,
        }
    }

    #[cfg(test)]
    fn with_cooldown(cooldown: Duration) -> Self {
        Self {
            cooldown,
            ..Self::new()
        }
    }

    /// Replace the cached set (called after listing the registry).
    /// Never arms the miss cooldown: a periodic or startup refresh must
    /// leave an unknown key able to trigger its own re-read (see the
    /// module doc for the cold-start failure that otherwise results).
    /// A read that enrols the key whose miss armed the cooldown gives
    /// that arming back (see the module doc).
    pub fn replace(&mut self, keys: impl IntoIterator<Item = String>) {
        self.keys = keys.into_iter().collect();
        if let Some((key, before)) = self.armed_by.take() {
            if self.keys.contains(&key) {
                self.last_miss_refresh = before;
            } else {
                self.armed_by = Some((key, before));
            }
        }
    }

    pub fn contains(&self, pubkey_hex: &str) -> bool {
        self.keys.contains(pubkey_hex)
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Whether a miss should trigger a registry refresh. `false` while
    /// inside the cooldown, so a flood of unknown keys cannot amplify
    /// into S3 list requests.
    pub fn refresh_due(&self) -> bool {
        match self.last_miss_refresh {
            None => true,
            Some(t) => t.elapsed() >= self.cooldown,
        }
    }

    /// Decide about `pubkey_hex`. Returns [`Decision::Refresh`] when the
    /// key is unknown and no miss has spent the cooldown yet; the caller
    /// then calls [`Allowlist::replace`] and asks again. Returning
    /// `Refresh` arms the cooldown, so the post-refresh re-ask (and any
    /// unknown-key flood behind it) rejects without another S3 list.
    pub fn check(&mut self, pubkey_hex: &str) -> Decision {
        if self.contains(pubkey_hex) {
            Decision::Accept
        } else if self.refresh_due() {
            let before = self.last_miss_refresh.replace(Instant::now());
            self.armed_by = Some((pubkey_hex.to_string(), before));
            Decision::Refresh
        } else {
            Decision::Reject
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Accept,
    /// Unknown key, but the registry cache is stale: refresh and retry.
    Refresh,
    Reject,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_key_is_accepted() {
        let mut a = Allowlist::new();
        a.replace(["aa".to_string(), "bb".to_string()]);
        assert_eq!(a.check("aa"), Decision::Accept);
        assert_eq!(a.len(), 2);
    }

    /// An unknown key asks for one refresh (a peer may have just
    /// enrolled), then is rejected until the cooldown expires.
    #[test]
    fn unknown_key_refreshes_once_then_rejects() {
        let mut a = Allowlist::with_cooldown(Duration::from_secs(3600));
        assert_eq!(a.check("ff"), Decision::Refresh, "cold cache must refresh");
        a.replace(["aa".to_string()]);
        assert_eq!(
            a.check("ff"),
            Decision::Reject,
            "still unknown after a fresh read: reject without re-listing"
        );
    }

    /// Once the cooldown lapses a miss may refresh again, so a peer that
    /// enrolls later is eventually admitted without a restart.
    #[test]
    fn refresh_allowed_again_after_cooldown() {
        let mut a = Allowlist::with_cooldown(Duration::from_millis(0));
        a.replace(["aa".to_string()]);
        assert_eq!(a.check("ff"), Decision::Refresh);
        a.replace(["aa".to_string(), "ff".to_string()]);
        assert_eq!(a.check("ff"), Decision::Accept);
    }

    #[test]
    fn empty_registry_rejects_everyone_but_still_refreshes() {
        let mut a = Allowlist::new();
        assert!(a.is_empty());
        assert_eq!(a.check("aa"), Decision::Refresh);
    }

    /// The cold-start race that flaked `forwarded-mutations`: node A
    /// reads the registry at startup (before B enrolled), then B dials
    /// in. A's periodic read must not have armed the cooldown, or B is
    /// rejected for the whole window, its forwarded mutations fail as
    /// transport errors, and the requester escalates to a lease
    /// takeover.
    #[test]
    fn periodic_refresh_does_not_block_a_new_peers_first_dial() {
        let mut a = Allowlist::with_cooldown(Duration::from_secs(3600));
        // Startup/periodic registry read: B not enrolled yet.
        a.replace(["aa".to_string()]);
        assert_eq!(
            a.check("bb"),
            Decision::Refresh,
            "a peer that enrolled after our periodic read deserves one re-read"
        );
        // The miss-triggered re-read finds B (it enrolled in between).
        a.replace(["aa".to_string(), "bb".to_string()]);
        assert_eq!(a.check("bb"), Decision::Accept);
    }

    /// Three mounts starting together: B's first dial refreshes and finds
    /// B, which must not lock C out for the cooldown — C enrolled a moment
    /// later and deserves its own re-read. A key that stays unknown still
    /// arms it.
    #[test]
    fn a_refresh_that_finds_its_key_spends_no_cooldown() {
        let mut a = Allowlist::with_cooldown(Duration::from_secs(3600));
        a.replace(["aa".to_string()]);
        assert_eq!(a.check("bb"), Decision::Refresh);
        a.replace(["aa".to_string(), "bb".to_string()]);
        assert_eq!(a.check("bb"), Decision::Accept);
        assert_eq!(
            a.check("cc"),
            Decision::Refresh,
            "B's productive refresh must not have armed the cooldown"
        );
        a.replace(["aa".to_string(), "bb".to_string()]);
        assert_eq!(
            a.check("cc"),
            Decision::Reject,
            "a futile refresh arms it: C is still unknown"
        );
        // A periodic read admits C: its miss was legitimate after all.
        a.replace(["aa".to_string(), "bb".to_string(), "cc".to_string()]);
        assert_eq!(a.check("cc"), Decision::Accept);
        // An unenrolled key gets one futile refresh, then the cooldown.
        assert_eq!(a.check("zz"), Decision::Refresh);
        a.replace(["aa".to_string(), "bb".to_string(), "cc".to_string()]);
        assert_eq!(a.check("zz"), Decision::Reject);
        assert_eq!(
            a.check("yy"),
            Decision::Reject,
            "nor may another unknown key"
        );
    }
}
