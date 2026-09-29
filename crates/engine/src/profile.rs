//! [`EngineProfile`]: how an [`crate::Engine`] spends its host (plan 31
//! §10).
//!
//! A profile is a set of budgets and modes chosen by the *host* (the
//! daemon, a CSI engine pod, a mobile app), not by the filesystem. The
//! type is defined in C4c so `Engine::start` has its final signature; the
//! lifecycle behaviour behind most of the modes (suspend/resume, forward-
//! only leases, metered-network upload deferral, on-demand background
//! work) is plan 31 C8's. What an engine honours today:
//!
//! - `memory_budget`/`cache_budget`: an explicit per-engine override of
//!   the equal share an [`crate::EngineHost`] would otherwise give it
//!   (§4.1; the `Server` knob of §10.1). `None`: the share.
//! - `p2p: Off` starts the engine without the P2P fast path (as
//!   `CONSTELLATION_P2P=off` does). `DialOnly` is served as `Listen` until
//!   C8 (the endpoint has no dial-only mode yet).
//!
//! `leases`, `uploads` and `background` are recorded and reported, and
//! change nothing until C8.

/// Whether the engine runs the P2P fast path, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pMode {
    /// Bind an endpoint, accept peers, join gossip (today's daemon).
    Listen,
    /// Dial peers but accept none (a phone behind carrier NAT). Served as
    /// `Listen` until C8.
    DialOnly,
    /// No P2P: S3 polling only.
    Off,
}

/// Whether the engine takes the write lease itself or forwards (C8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseMode {
    /// Acquire and hold leases (today's daemon).
    Hold,
    /// Forward every mutation to a holder; never acquire (C8).
    ForwardOnly,
}

/// When chunk uploads run (C8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadMode {
    Always,
    /// Only on an unmetered network (C8).
    UnmeteredOnly,
}

/// Whether background work (GC, prune, publish, digests) runs all the
/// time or only while a view is in use (C8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundMode {
    Continuous,
    OnDemand,
}

/// The host's budgets and modes for one engine (plan 31 §10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineProfile {
    /// Memory this engine may use; `None` takes the host's equal share
    /// ([`crate::ResourceBudget::share`]).
    pub memory_budget: Option<u64>,
    /// Chunk-cache bytes; `None` takes the host's equal share (and never
    /// more than the engine's own `EngineConfig::cache_size`).
    pub cache_budget: Option<u64>,
    pub p2p: P2pMode,
    pub leases: LeaseMode,
    pub uploads: UploadMode,
    pub background: BackgroundMode,
}

impl EngineProfile {
    /// A desktop/server daemon as it has always run: P2P listening,
    /// leases held, uploads always, background work continuous, budgets
    /// from the host's share.
    pub fn desktop() -> Self {
        Self {
            memory_budget: None,
            cache_budget: None,
            p2p: P2pMode::Listen,
            leases: LeaseMode::Hold,
            uploads: UploadMode::Always,
            background: BackgroundMode::Continuous,
        }
    }

    /// §10.1's `Server` preset for dense multi-filesystem hosts (plan 37's
    /// engine pods): the desktop modes, with explicit budgets sized for a
    /// long-lived process — the per-engine override of the host's equal
    /// share. No new fields: a particular combination of the existing
    /// ones.
    pub fn server(memory_budget: u64, cache_budget: u64) -> Self {
        Self {
            memory_budget: Some(memory_budget),
            cache_budget: Some(cache_budget),
            ..Self::desktop()
        }
    }

    /// Whether this profile starts the P2P fast path.
    pub fn p2p_enabled(&self) -> bool {
        self.p2p != P2pMode::Off
    }
}

impl Default for EngineProfile {
    /// [`EngineProfile::desktop`].
    fn default() -> Self {
        Self::desktop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_is_desktop_with_explicit_budgets() {
        let desktop = EngineProfile::default();
        assert_eq!(desktop, EngineProfile::desktop());
        assert_eq!((desktop.memory_budget, desktop.cache_budget), (None, None));
        assert!(desktop.p2p_enabled());
        let server = EngineProfile::server(8 << 30, 64 << 30);
        assert_eq!(server.memory_budget, Some(8 << 30));
        assert_eq!(server.cache_budget, Some(64 << 30));
        assert_eq!(
            (server.p2p, server.leases, server.uploads, server.background),
            (
                P2pMode::Listen,
                LeaseMode::Hold,
                UploadMode::Always,
                BackgroundMode::Continuous
            )
        );
        let off = EngineProfile {
            p2p: P2pMode::Off,
            ..EngineProfile::desktop()
        };
        assert!(!off.p2p_enabled());
    }
}
