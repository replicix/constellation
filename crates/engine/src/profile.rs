//! [`EngineProfile`]: how an [`crate::Engine`] spends its host (plan 31
//! §10).
//!
//! A profile is a set of budgets and modes chosen by the *host* (the
//! daemon, a CSI engine pod, a mobile app), not by the filesystem. What
//! each field does (plan 31 C8; [`crate::lifecycle`] applies the modes as
//! the host's lifecycle events arrive):
//!
//! | field | value | effect |
//! |---|---|---|
//! | `memory_budget`/`cache_budget` | `Some(n)` | an explicit per-engine override of the equal share an [`crate::EngineHost`] would give it (§4.1; `None`: the share) |
//! | `p2p` | `Listen` | bind an endpoint, accept peers, join gossip |
//! | | `DialOnly` | the same endpoint, but every *inbound* connection is refused right after its handshake: this node dials (forwards, chunk fetches, its log-stream subscription, gossip's own links) and is never dialed |
//! | | `Off` | no endpoint: S3 polling and the S3 inbox only |
//! | `leases` | `Hold` | take and hold the write lease as ever |
//! | | `ForwardOnly` | never take the lease from a live holder (no handoff request, no `wanted_by`, no placement offer, no inbox escalation), forward every mutation to the holder, and give back a lease taken because nobody held one as soon as it is idle |
//! | `uploads` | `Always` | chunk uploads run whenever there is something to upload |
//! | | `UnmeteredOnly` | while the host's last `NetworkChanged` said metered, opportunistic chunk uploads hold (closes journal locally, as `--write-mode back` does); explicit durability requests still upload |
//! | `background` | `Continuous` | GC, prune, digests, registry and designation polls run all the time (paused only while suspended) |
//! | | `OnDemand` | they also pause while the host is in the background or low on power |
//!
//! `CONSTELLATION_PROFILE` and its per-field overrides pick a profile for
//! the daemon ([`EngineProfile::from_env`]).

use constellation_fs_core::cache::CacheVerify;

/// Env `CONSTELLATION_CACHE_VERIFY`: overrides `--cache-verify`
/// ([`cache_verify`]).
pub const CACHE_VERIFY_ENV: &str = "CONSTELLATION_CACHE_VERIFY";

/// When a disk-cache read re-hashes the file it read (plan 38 §2.3):
/// `CONSTELLATION_CACHE_VERIFY`, else the `--cache-verify` flag, else
/// [`CacheVerify::Admit`].
///
/// The env override wins, matching `CONSTELLATION_ATIME`/`CONSTELLATION_CTO`,
/// so an operator can put a suspect host into `always` without editing
/// the registry row its mount flags come from. An unparseable value
/// falls through to the next source rather than failing the mount — a
/// stale `CONSTELLATION_CACHE_VERIFY` in a shell profile would otherwise
/// brick every mount from it — but it is *warned about*, because the
/// fall-through is in the less careful direction: someone typing
/// `always` to harden a suspect host must not silently stay on `admit`.
pub fn cache_verify(flag: Option<CacheVerify>) -> CacheVerify {
    cache_verify_from(std::env::var(CACHE_VERIFY_ENV).ok().as_deref(), flag)
}

fn cache_verify_from(var: Option<&str>, flag: Option<CacheVerify>) -> CacheVerify {
    let parsed = var.and_then(CacheVerify::parse);
    if parsed.is_none() {
        if let Some(raw) = var.map(str::trim).filter(|raw| !raw.is_empty()) {
            tracing::warn!(
                value = raw,
                env = CACHE_VERIFY_ENV,
                "ignoring an unparseable cache verification mode (expected admit or always)"
            );
        }
    }
    parsed.or(flag).unwrap_or_default()
}

/// Env `CONSTELLATION_CHUNK_MEMCACHE_BYTES`: the chunk memory cache's
/// byte budget, overriding [`EngineProfile::chunk_memcache_default`];
/// `0` turns it off.
pub const CHUNK_MEMCACHE_ENV: &str = "CONSTELLATION_CHUNK_MEMCACHE_BYTES";

/// The chunk memory cache budget an engine runs with: the env override
/// or the profile's default for `memory`, never more than the disk
/// cache's `disk_budget` (memory holds a subset of the disk's chunks).
pub fn chunk_memcache_bytes(profile: &EngineProfile, memory: u64, disk_budget: u64) -> u64 {
    chunk_memcache_bytes_from(
        std::env::var(CHUNK_MEMCACHE_ENV).ok().as_deref(),
        profile,
        memory,
        disk_budget,
    )
}

fn chunk_memcache_bytes_from(
    var: Option<&str>,
    profile: &EngineProfile,
    memory: u64,
    disk_budget: u64,
) -> u64 {
    var.and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or_else(|| profile.chunk_memcache_default(memory))
        .min(disk_budget)
}

/// Whether the engine runs the P2P fast path, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pMode {
    /// Bind an endpoint, accept peers, join gossip (today's daemon).
    Listen,
    /// Dial peers but accept none (a phone behind carrier NAT, or asleep
    /// behind Doze): inbound connections are refused after their
    /// handshake (`constellation_net::Peers::set_dial_only`).
    DialOnly,
    /// No P2P: S3 polling only.
    Off,
}

/// Whether the engine takes the write lease itself or forwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseMode {
    /// Acquire and hold leases (today's daemon).
    Hold,
    /// Forward every mutation to a holder; never take the lease from a
    /// live one, and give back one taken because nobody held any as soon
    /// as it is idle (`constellation_authority::AuthorityMode`).
    ForwardOnly,
}

/// When chunk uploads run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadMode {
    Always,
    /// Only on an unmetered network: opportunistic uploads hold while the
    /// host's last `NetworkChanged` said metered.
    UnmeteredOnly,
}

/// Whether background work (GC, prune, digests, registry and designation
/// polls) runs all the time or only while the host is in the foreground.
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

    /// A phone (plan 36 settled decisions 8 and the battery policy):
    /// dial-only P2P, forward-only leases, uploads on unmetered networks
    /// only, background work only in the foreground. Budgets are the
    /// host's share until plan 36's A1 tunes them against real devices.
    pub fn mobile() -> Self {
        Self {
            memory_budget: None,
            cache_budget: None,
            p2p: P2pMode::DialOnly,
            leases: LeaseMode::ForwardOnly,
            uploads: UploadMode::UnmeteredOnly,
            background: BackgroundMode::OnDemand,
        }
    }

    /// Default byte budget of the chunk memory cache
    /// (`constellation_fs_core::memcache`) for an engine that may use
    /// `memory` bytes (its host share, or this profile's explicit
    /// `memory_budget`). Modest on purpose: the disk cache already holds
    /// every chunk, and the memory tier only has to hold the chunks being
    /// read right now plus a hot set.
    ///
    /// * explicit `memory_budget` (the `Server` preset): an eighth of it;
    /// * otherwise 1/64 of the share, capped at 128 MiB (a desktop), or at
    ///   16 MiB with `BackgroundMode::OnDemand` (a phone: its host
    ///   suspends it and memory is tight).
    pub fn chunk_memcache_default(&self, memory: u64) -> u64 {
        const MIB: u64 = 1024 * 1024;
        if self.memory_budget.is_some() {
            return memory / 8;
        }
        let cap = match self.background {
            BackgroundMode::OnDemand => 16 * MIB,
            BackgroundMode::Continuous => 128 * MIB,
        };
        (memory / 64).min(cap)
    }

    /// Whether this profile starts the P2P fast path.
    pub fn p2p_enabled(&self) -> bool {
        self.p2p != P2pMode::Off
    }

    /// The profile a daemon runs with: `base`, or the preset named by
    /// `CONSTELLATION_PROFILE` (`desktop`, `server`, `mobile`), then each
    /// mode overridden by its own variable —
    /// `CONSTELLATION_PROFILE_P2P` (`listen`/`dial-only`/`off`),
    /// `CONSTELLATION_PROFILE_LEASES` (`hold`/`forward-only`),
    /// `CONSTELLATION_PROFILE_UPLOADS` (`always`/`unmetered-only`),
    /// `CONSTELLATION_PROFILE_BACKGROUND` (`continuous`/`on-demand`).
    /// `server` here keeps `base`'s budgets (the host's share); an
    /// explicit budget is `EngineProfile::server`'s argument. An unknown
    /// value is an error, not a silent default.
    pub fn from_env(base: EngineProfile) -> Result<EngineProfile, String> {
        Self::from_vars(base, |key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] over any variable source (tests).
    pub fn from_vars(
        base: EngineProfile,
        var: impl Fn(&str) -> Option<String>,
    ) -> Result<EngineProfile, String> {
        let get = |key: &str| {
            var(key)
                .map(|v| v.trim().to_ascii_lowercase())
                .filter(|v| !v.is_empty())
        };
        let bad =
            |key: &str, value: &str, expected: &str| format!("{key}={value}: expected {expected}");
        let mut profile = match get("CONSTELLATION_PROFILE").as_deref() {
            None => base,
            // `server` is `desktop`'s modes (§10.1); its budgets are the
            // base's here.
            Some("desktop" | "server") => Self {
                memory_budget: base.memory_budget,
                cache_budget: base.cache_budget,
                ..Self::desktop()
            },
            Some("mobile") => Self {
                memory_budget: base.memory_budget,
                cache_budget: base.cache_budget,
                ..Self::mobile()
            },
            Some(other) => {
                return Err(bad(
                    "CONSTELLATION_PROFILE",
                    other,
                    "desktop, server or mobile",
                ))
            }
        };
        if let Some(v) = get("CONSTELLATION_PROFILE_P2P") {
            profile.p2p = v
                .parse()
                .map_err(|e: &str| bad("CONSTELLATION_PROFILE_P2P", &v, e))?;
        }
        if let Some(v) = get("CONSTELLATION_PROFILE_LEASES") {
            profile.leases = v
                .parse()
                .map_err(|e: &str| bad("CONSTELLATION_PROFILE_LEASES", &v, e))?;
        }
        if let Some(v) = get("CONSTELLATION_PROFILE_UPLOADS") {
            profile.uploads = v
                .parse()
                .map_err(|e: &str| bad("CONSTELLATION_PROFILE_UPLOADS", &v, e))?;
        }
        if let Some(v) = get("CONSTELLATION_PROFILE_BACKGROUND") {
            profile.background = v
                .parse()
                .map_err(|e: &str| bad("CONSTELLATION_PROFILE_BACKGROUND", &v, e))?;
        }
        Ok(profile)
    }
}

/// Names as `status` prints them and the environment spells them.
macro_rules! mode_names {
    ($ty:ty, $expected:literal, $($variant:ident => $name:literal),+ $(,)?) => {
        impl $ty {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }
        }

        impl std::str::FromStr for $ty {
            type Err = &'static str;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($name => Ok(Self::$variant),)+
                    _ => Err($expected),
                }
            }
        }
    };
}

mode_names!(P2pMode, "listen, dial-only or off",
    Listen => "listen", DialOnly => "dial-only", Off => "off");
mode_names!(LeaseMode, "hold or forward-only",
    Hold => "hold", ForwardOnly => "forward-only");
mode_names!(UploadMode, "always or unmetered-only",
    Always => "always", UnmeteredOnly => "unmetered-only");
mode_names!(BackgroundMode, "continuous or on-demand",
    Continuous => "continuous", OnDemand => "on-demand");

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

    #[test]
    fn mobile_is_every_constrained_mode() {
        let mobile = EngineProfile::mobile();
        assert_eq!(
            (mobile.p2p, mobile.leases, mobile.uploads, mobile.background),
            (
                P2pMode::DialOnly,
                LeaseMode::ForwardOnly,
                UploadMode::UnmeteredOnly,
                BackgroundMode::OnDemand
            )
        );
        assert!(mobile.p2p_enabled(), "dial-only still runs an endpoint");
    }

    #[test]
    fn the_environment_picks_a_preset_then_overrides_each_mode() {
        let vars = |pairs: &'static [(&'static str, &'static str)]| {
            move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.to_string())
            }
        };
        let base = EngineProfile::server(1 << 30, 2 << 30);
        assert_eq!(
            EngineProfile::from_vars(base.clone(), vars(&[])),
            Ok(base.clone())
        );
        let mobile =
            EngineProfile::from_vars(base.clone(), vars(&[("CONSTELLATION_PROFILE", " Mobile ")]))
                .unwrap();
        assert_eq!(mobile.leases, LeaseMode::ForwardOnly);
        assert_eq!(
            mobile.cache_budget,
            Some(2 << 30),
            "the base's budgets stay"
        );
        let mixed = EngineProfile::from_vars(
            EngineProfile::desktop(),
            vars(&[
                ("CONSTELLATION_PROFILE_P2P", "dial-only"),
                ("CONSTELLATION_PROFILE_LEASES", "forward-only"),
                ("CONSTELLATION_PROFILE_UPLOADS", "unmetered-only"),
                ("CONSTELLATION_PROFILE_BACKGROUND", "on-demand"),
            ]),
        )
        .unwrap();
        assert_eq!(
            (mixed.p2p, mixed.leases, mixed.uploads, mixed.background),
            (
                P2pMode::DialOnly,
                LeaseMode::ForwardOnly,
                UploadMode::UnmeteredOnly,
                BackgroundMode::OnDemand
            )
        );
        for (key, value) in [
            ("CONSTELLATION_PROFILE", "phone"),
            ("CONSTELLATION_PROFILE_P2P", "listen-only"),
            ("CONSTELLATION_PROFILE_LEASES", "forward"),
        ] {
            let err = EngineProfile::from_vars(EngineProfile::desktop(), |k: &str| {
                (k == key).then(|| value.to_string())
            })
            .unwrap_err();
            assert!(err.contains(key) && err.contains(value), "{err}");
        }
        for mode in [P2pMode::Listen, P2pMode::DialOnly, P2pMode::Off] {
            assert_eq!(mode.as_str().parse::<P2pMode>(), Ok(mode));
        }
    }

    #[test]
    fn the_chunk_memcache_default_is_modest_and_the_knob_overrides_it() {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * MIB;
        let desktop = EngineProfile::desktop();
        // 1/64 of the share, capped at 128 MiB; a phone at 16 MiB.
        assert_eq!(desktop.chunk_memcache_default(4 * GIB), 64 * MIB);
        assert_eq!(desktop.chunk_memcache_default(64 * GIB), 128 * MIB);
        assert_eq!(desktop.chunk_memcache_default(u64::MAX), 128 * MIB);
        assert_eq!(
            EngineProfile::mobile().chunk_memcache_default(6 * GIB),
            16 * MIB
        );
        assert_eq!(
            EngineProfile::mobile().chunk_memcache_default(512 * MIB),
            8 * MIB
        );
        // An explicit memory budget: an eighth of it.
        let server = EngineProfile::server(8 * GIB, 100 * GIB);
        assert_eq!(server.chunk_memcache_default(8 * GIB), GIB);
        // The knob wins (0: off); never more than the disk cache.
        let pick = |var, disk| chunk_memcache_bytes_from(var, &desktop, 16 * GIB, disk);
        assert_eq!(pick(None, 100 * GIB), 128 * MIB);
        assert_eq!(pick(Some("0"), 100 * GIB), 0);
        assert_eq!(pick(Some("536870912"), 100 * GIB), 512 * MIB);
        assert_eq!(pick(Some("536870912"), 64 * MIB), 64 * MIB);
        assert_eq!(pick(None, 16 * MIB), 16 * MIB);
        assert_eq!(
            pick(Some("lots"), 100 * GIB),
            128 * MIB,
            "unparsable: the default"
        );
    }

    #[test]
    fn cache_verify_env_overrides_the_flag() {
        use CacheVerify::*;
        assert_eq!(cache_verify_from(None, None), Admit);
        assert_eq!(cache_verify_from(None, Some(Always)), Always);
        assert_eq!(cache_verify_from(Some("always"), None), Always);
        // The env wins over the mount flag, as `CONSTELLATION_ATIME` does.
        assert_eq!(cache_verify_from(Some("admit"), Some(Always)), Admit);
        assert_eq!(cache_verify_from(Some(" ALWAYS "), Some(Admit)), Always);
        // Unparseable: fall through rather than refuse to serve.
        assert_eq!(cache_verify_from(Some("maybe"), Some(Always)), Always);
        assert_eq!(cache_verify_from(Some("maybe"), None), Admit);
    }
}
