//! The four knobs that decide a tree's shape, plus the projection that
//! turns a leaf entry into a §P7 aggregate.
//!
//! These are a *format* parameter set, not a tuning surface: two trees
//! built over the same key set with different clamps or a different
//! hasher are different trees with different root hashes. So a
//! filesystem picks a `Config` once, and everything that later reads
//! its bucket has to pick the same one. The defaults are what §14
//! measured, and the only field a production caller is expected to vary
//! is [`Config::hasher`] (E2E or not, §P13) and [`Config::leaf_agg`]
//! (which needs the §P6 key codec, so it cannot be defaulted usefully
//! at this layer).

use crate::error::MtreeError;
use crate::hash::Hasher;
use crate::node::{self, Agg, MAX_ENTRIES, MIN_ENTRIES};

/// Per-entry contribution to the §P7 interior aggregates.
///
/// A function pointer supplied by the caller rather than a trait or a
/// generic parameter, because the decision it encodes belongs to the
/// key codec, not to the structure: only the codec knows which key
/// range holds the authoritative inode record, and therefore which
/// entries may count a file's bytes without double-counting a
/// denormalized copy (§P6's dentry attr copy, §14.8's open question).
/// `mtree` must not be able to guess.
///
/// The projection must be a pure function of the entry, or the tree
/// stops being canonical.
pub type LeafAgg = fn(key: &[u8], value: &[u8]) -> Agg;

/// The default projection: count keys, attribute no bytes, no files,
/// no mtime. An `mtree` used as a plain map gets exact key counts and
/// nothing else, which is honest — inventing byte totals from opaque
/// values would be worse than reporting none.
pub fn no_leaf_agg(_key: &[u8], _value: &[u8]) -> Agg {
    Agg::EMPTY
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Plain or keyed blake3 (§P13). Governs node identity *and* the
    /// boundary function — see [`crate::hash`].
    pub hasher: Hasher,
    /// Boundaries inside the first `min_entries` of a node are
    /// suppressed. 1 disables the lower clamp.
    pub min_entries: usize,
    /// A node is sealed once it holds `max_entries`, boundary key or
    /// not. §14.1's 256.
    pub max_entries: usize,
    pub leaf_agg: LeafAgg,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            hasher: Hasher::Plain,
            min_entries: MIN_ENTRIES,
            max_entries: MAX_ENTRIES,
            leaf_agg: no_leaf_agg,
        }
    }
}

impl Config {
    /// E2E addressing: node hashes and boundary tests both keyed.
    pub fn keyed(key: [u8; 32]) -> Config {
        Config {
            hasher: Hasher::Keyed(key),
            ..Config::default()
        }
    }

    pub fn with_hasher(mut self, hasher: Hasher) -> Config {
        self.hasher = hasher;
        self
    }

    pub fn with_leaf_agg(mut self, leaf_agg: LeafAgg) -> Config {
        self.leaf_agg = leaf_agg;
        self
    }

    pub fn with_entry_clamps(mut self, min: usize, max: usize) -> Config {
        self.min_entries = min;
        self.max_entries = max;
        self
    }

    pub fn validate(&self) -> Result<(), MtreeError> {
        if self.min_entries == 0 {
            return Err(MtreeError::Config("min_entries must be at least 1"));
        }
        if self.max_entries < self.min_entries {
            return Err(MtreeError::Config("max_entries is below min_entries"));
        }
        Ok(())
    }

    pub(crate) fn is_boundary(&self, key: &[u8], level: u8) -> bool {
        node::is_boundary(&self.hasher, key, level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonsensical_clamps_are_refused() {
        assert!(Config::default().validate().is_ok());
        assert!(Config::default()
            .with_entry_clamps(0, 256)
            .validate()
            .is_err());
        assert!(Config::default()
            .with_entry_clamps(300, 256)
            .validate()
            .is_err());
        assert!(Config::default().with_entry_clamps(8, 8).validate().is_ok());
    }

    #[test]
    fn the_defaults_are_what_section_14_measured() {
        let config = Config::default();
        assert_eq!(config.min_entries, 1);
        assert_eq!(config.max_entries, 256);
        assert_eq!(config.hasher, Hasher::Plain);
    }
}
