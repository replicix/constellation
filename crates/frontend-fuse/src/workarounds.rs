//! Plan 42 §5.4: the settings of the Linux FUSE kernel-race workarounds,
//! read once when the daemon starts (with the transport knob,
//! [`crate::TransportConfig::resolve`]) and carried to every mount. An
//! unparseable value fails the mount, as an unparseable
//! `CONSTELLATION_FUSE_TRANSPORT` does: each is a choice between a
//! workaround and its cost, and a silent default would be the opposite
//! of one of them.

use crate::pagecache::{KeepCache, PageCacheConfig};

/// `CONSTELLATION_FUSE_KEEP_CACHE`: `auto` (default) or `off`.
pub const KEEP_CACHE_ENV: &str = "CONSTELLATION_FUSE_KEEP_CACHE";

/// `CONSTELLATION_FAULT_KEEP_CACHE_IGNORE_SUSPECT=1` (test-only).
pub const IGNORE_SUSPECT_FAULT_ENV: &str = "CONSTELLATION_FAULT_KEEP_CACHE_IGNORE_SUSPECT";

/// `CONSTELLATION_FUSE_WRITE_REPAIR`: `auto` (default), `on` or `off`.
pub const WRITE_REPAIR_ENV: &str = "CONSTELLATION_FUSE_WRITE_REPAIR";

/// `CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS`: the repair's bound.
pub const WRITE_REPAIR_WAIT_ENV: &str = "CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS";

/// `CONSTELLATION_FAULT_UNSEEN_DROP_MS=N` (test-only): see
/// [`Workarounds::unseen_drop`].
pub const UNSEEN_DROP_FAULT_ENV: &str = "CONSTELLATION_FAULT_UNSEEN_DROP_MS";

/// The kernel's page size.
pub(crate) fn page_size() -> u64 {
    // SAFETY: sysconf has no preconditions.
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if p > 0 && (p as u64).is_power_of_two() {
        p as u64
    } else {
        4096
    }
}

/// An `auto`/`on`/`off` setting: `auto` is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Switch {
    Auto,
    On,
    Off,
}

impl Switch {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Some(Self::Auto),
            "on" => Some(Self::On),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

/// The workarounds' settings (module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Workarounds {
    /// W1, plan 42 §5.1.
    pub keep_cache: KeepCache,
    /// The fault-only naive keep-cache of §7.2 (ignores S5).
    pub ignore_s5: bool,
    /// W2's repair, plan 42 §5.2.3 (`auto`: on).
    pub write_repair: Switch,
    /// The repair's in-flight-aware bound.
    pub repair_wait: std::time::Duration,
    /// Test-only: every this often, the session drops the pages of every
    /// file open for writing with a data notification nothing announces —
    /// no arm, no mark — as memory reclaim would if it could drop a folio
    /// a writer holds (it cannot: `crate::pagecache`'s module doc). The
    /// drop the write-race bookkeeping cannot see, for the
    /// `kernel-race-unseen-drop` scenario.
    pub unseen_drop: Option<std::time::Duration>,
}

impl Default for Workarounds {
    fn default() -> Self {
        Self {
            keep_cache: KeepCache::Auto,
            ignore_s5: false,
            write_repair: Switch::Auto,
            repair_wait: crate::repair::DEFAULT_WAIT,
            unseen_drop: None,
        }
    }
}

impl Workarounds {
    /// From the variables `var` answers (the environment, or a test's).
    pub fn resolve_from(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let get = |key: &str| {
            var(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let keep_cache = match get(KEEP_CACHE_ENV) {
            Some(raw) => KeepCache::parse(&raw)
                .ok_or_else(|| format!("{KEEP_CACHE_ENV}={raw}: expected auto or off"))?,
            None => KeepCache::Auto,
        };
        let ignore_s5 = get(IGNORE_SUSPECT_FAULT_ENV).is_some_and(|v| v == "1");
        if ignore_s5 {
            tracing::warn!(
                "{IGNORE_SUSPECT_FAULT_ENV}=1: opens keep pages the write-race bookkeeping could not \
                 vouch for (a fault-injection knob; never set it on a real filesystem)"
            );
        }
        let write_repair = match get(WRITE_REPAIR_ENV) {
            Some(raw) => Switch::parse(&raw)
                .ok_or_else(|| format!("{WRITE_REPAIR_ENV}={raw}: expected auto, on or off"))?,
            None => Switch::Auto,
        };
        let repair_wait = match get(WRITE_REPAIR_WAIT_ENV) {
            Some(raw) => std::time::Duration::from_millis(
                raw.parse::<u64>()
                    .ok()
                    .filter(|ms| *ms > 0)
                    .ok_or_else(|| {
                        format!("{WRITE_REPAIR_WAIT_ENV}={raw}: expected a positive number of ms")
                    })?,
            ),
            None => crate::repair::DEFAULT_WAIT,
        };
        let unseen_drop = get(UNSEEN_DROP_FAULT_ENV)
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(std::time::Duration::from_millis);
        if unseen_drop.is_some() {
            tracing::warn!(
                "{UNSEEN_DROP_FAULT_ENV}: the session drops written files' pages unannounced \
                 (a fault-injection knob; never set it on a real filesystem)"
            );
        }
        Ok(Self {
            keep_cache,
            ignore_s5,
            write_repair,
            repair_wait,
            unseen_drop,
        })
    }

    /// What the session's page-cache table works under.
    pub(crate) fn page_cache(&self) -> PageCacheConfig {
        PageCacheConfig {
            keep_cache: self.keep_cache,
            ignore_s5: self.ignore_s5,
            write_repair: self.write_repair,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_settings_parse_and_refuse_garbage() {
        let none = |_: &str| None;
        assert_eq!(
            Workarounds::resolve_from(none).unwrap(),
            Workarounds::default()
        );
        let off = |k: &str| (k == KEEP_CACHE_ENV).then(|| "off".to_string());
        assert_eq!(
            Workarounds::resolve_from(off).unwrap().keep_cache,
            KeepCache::Off
        );
        let bad = |k: &str| (k == KEEP_CACHE_ENV).then(|| "sometimes".to_string());
        assert!(Workarounds::resolve_from(bad)
            .unwrap_err()
            .contains(KEEP_CACHE_ENV));
    }
}
