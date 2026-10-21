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

/// The workarounds' settings (module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Workarounds {
    /// W1, plan 42 §5.1.
    pub keep_cache: KeepCache,
    /// The fault-only naive keep-cache of §7.2 (ignores S5).
    pub ignore_s5: bool,
}

impl Default for Workarounds {
    fn default() -> Self {
        Self {
            keep_cache: KeepCache::Auto,
            ignore_s5: false,
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
        Ok(Self {
            keep_cache,
            ignore_s5,
        })
    }

    /// What the session's page-cache table works under.
    pub(crate) fn page_cache(&self) -> PageCacheConfig {
        PageCacheConfig {
            keep_cache: self.keep_cache,
            ignore_s5: self.ignore_s5,
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
