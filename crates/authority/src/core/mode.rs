//! Plan 31 C8: how this node may take write authority — the engine
//! profile's `LeaseMode` and a host suspension, as the core sees them.
//!
//! Neither mode adds a new path through the core. Both only close some of
//! the existing ways *into* holding the lease, so that every op this node
//! cannot execute takes the non-holder paths the core already has
//! (forward to the holder, M13's S3 inbox with no P2P path, the
//! read-your-refusal wait, the deadline's `InDoubt`):
//!
//! - **forward-only** (`LeaseMode::ForwardOnly`, a phone's profile — plan
//!   36 settled decision 8): never take the lease *from a live holder* —
//!   no P2P handoff request, no `wanted_by` registration (which would make
//!   the holder hand it over), no placement offer claimed, no inbox
//!   escalation, no re-adoption of an old tenure for a forward — and give
//!   a lease this node did take back as soon as it is idle (its journal
//!   shipped and the minimum dwell over) instead of waiting to be asked.
//!   Routing always forwards first, whatever `CONSTELLATION_FORWARD`
//!   says. A lease nobody holds (none, released, expired) is still
//!   claimable: refusing it would leave the cluster with no sequencer at
//!   all and every write of this node failing, which a desktop that
//!   happens to be off must not do to a phone.
//! - **suspended** (`Suspending` until `Resumed`): forward-only, and no
//!   acquisition at all except the two that are the protocol's own safety
//!   duties — a sealed backup's takeover (it alone may hold an
//!   acknowledged tail the log does not have yet, plan 30 §M9) and a
//!   continuation epoch's flush re-claim (its journal must reach S3 under
//!   the lease it carried, §M10). Ops that need a holder while suspended
//!   wait for another node to take the lease, then forward to it, exactly
//!   as a non-holder's would; one still waiting at its deadline ends as
//!   every such op does (`InDoubt`).
//!
//! What the modes deliberately leave alone: promises (M10 — they help a
//! taker in, never hold one back), backup duties (M9 — a backup that
//! stopped acking would stall its holder's acknowledgements until the
//! reconfiguration; the holder drops a backup it cannot reach on its own
//! schedule), lease renewal of a tenure already held (a suspension's
//! flush releases it cleanly; renewal until then is what keeps the
//! tenure's acknowledged work ordered).

/// See the module docs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuthorityMode {
    /// The profile's `LeaseMode::ForwardOnly`.
    pub forward_only: bool,
    /// A host suspension is in force.
    pub suspended: bool,
}

impl AuthorityMode {
    /// Never take the lease from a live holder; release when idle.
    pub fn forwards(&self) -> bool {
        self.forward_only || self.suspended
    }

    /// Whether an acquisition for `reason` may run at all, and whether it
    /// may ask a live holder to hand over (`ask_handoff` as requested,
    /// narrowed by the mode). `None`: refused outright.
    pub fn admit_acquire(&self, reason: &'static str, ask_handoff: bool) -> Option<bool> {
        // The safety duties (see the module docs): always admitted, and
        // never an ask of a live holder anyway.
        const DUTIES: [&str; 2] = ["backup-takeover", "epoch-close-reclaim"];
        if DUTIES.contains(&reason) {
            return Some(ask_handoff);
        }
        if self.suspended {
            return None;
        }
        if self.forward_only {
            // Asking for the lease is the point of these; a forward-only
            // node forwards instead.
            if matches!(reason, "claim-offer" | "inbox-escalation") {
                return None;
            }
            return Some(false);
        }
        Some(ask_handoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_admits_everything_as_asked() {
        let hold = AuthorityMode::default();
        assert!(!hold.forwards());
        for reason in ["fuse-acquire", "claim-offer", "inbox-escalation", "lock"] {
            assert_eq!(hold.admit_acquire(reason, true), Some(true), "{reason}");
            assert_eq!(hold.admit_acquire(reason, false), Some(false), "{reason}");
        }
    }

    #[test]
    fn forward_only_never_asks_a_live_holder_and_never_claims_an_offer() {
        let mode = AuthorityMode {
            forward_only: true,
            suspended: false,
        };
        assert!(mode.forwards());
        for reason in [
            "fuse-acquire",
            "control-acquire",
            "ship-pending-journal",
            "lock",
            "replay-fallback",
            "copy-fallback",
        ] {
            assert_eq!(mode.admit_acquire(reason, true), Some(false), "{reason}");
        }
        assert_eq!(mode.admit_acquire("claim-offer", true), None);
        assert_eq!(mode.admit_acquire("inbox-escalation", true), None);
        assert_eq!(mode.admit_acquire("backup-takeover", false), Some(false));
    }

    #[test]
    fn suspended_admits_only_the_safety_duties() {
        let mode = AuthorityMode {
            forward_only: false,
            suspended: true,
        };
        assert!(mode.forwards());
        for reason in [
            "fuse-acquire",
            "control-acquire",
            "ship-pending-journal",
            "claim-offer",
            "s3-fast-takeover",
            "lock",
        ] {
            assert_eq!(mode.admit_acquire(reason, true), None, "{reason}");
        }
        assert_eq!(mode.admit_acquire("backup-takeover", false), Some(false));
        assert_eq!(
            mode.admit_acquire("epoch-close-reclaim", false),
            Some(false)
        );
    }
}
