//! The cookie of a directory entry (plan 40 §4.9): a keyed hash of its
//! name, so a listing resumes after the *name* it stopped at and a user
//! cannot craft names whose cookies collide.
//!
//! `cookie(name) = (siphash13(k, name) >> 2) + 3`: in `[3, 2^62 + 2]`,
//! clear of 0 (rewind), 1 (after `.`) and 2 (after `..`), and below
//! `i64::MAX` (fuser's offsets are signed). The key `k` belongs to the
//! filesystem and is never rotated, so a name has the same cookie in every
//! view, every stream, every node, across a restart and a handover.
//!
//! The key is **derived** from the secret material the filesystem already
//! stores, so `meta.json` does not change:
//!
//! - E2E: the master-derived subkey `dek("readdir/cookie-v1")` (the `/`
//!   keeps it disjoint from the partition names the other DEKs are named
//!   by);
//! - otherwise `blake3::derive_key("constellation readdir/cookie-v1",
//!   gossip_secret)`.
//!
//! A non-E2E filesystem without a `gossip_secret` (created before the field
//! existed) has no key and the engine does not start on it: a fallback to
//! the UUID would be a key anyone who can read the bucket's name knows.
//!
//! The 32-byte result is truncated to the two `u64` keys SipHash takes.

use anyhow::{bail, Result};
use constellation_store_s3::{FsMeta, SharedE2eKeys};
use siphasher::sip::SipHasher13;

/// Env `CONSTELLATION_READDIR_COOKIE_BITS`: `62` (default) or `31`.
pub const COOKIE_BITS_ENV: &str = "CONSTELLATION_READDIR_COOKIE_BITS";

/// How wide a cookie is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CookieBits {
    /// `[3, 2^62 + 2]`.
    #[default]
    Wide,
    /// `[3, 2^31 - 2]`, for 32-bit userspace: non-LFS 32-bit glibc fails
    /// `getdents` with `EOVERFLOW` on a `d_off` above 2^31 - 1, and
    /// `telldir`/`seekdir` (`long`) cannot round-trip 62 bits. Collisions
    /// are common at scale (~0.5 % of the entries of a 10M directory) but
    /// only reach the cold path (a name listed twice, never skipped), as a
    /// page never holds two equal cookies.
    Narrow,
}

impl CookieBits {
    /// The mode `CONSTELLATION_READDIR_COOKIE_BITS` names; any value but
    /// `62` and `31` is an error (the start fails, so a typo cannot leave
    /// a 32-bit fleet on 62-bit cookies).
    pub fn parse(var: Option<&str>) -> Result<Self> {
        match var.map(str::trim) {
            None | Some("") | Some("62") => Ok(Self::Wide),
            Some("31") => Ok(Self::Narrow),
            Some(other) => bail!("{COOKIE_BITS_ENV}={other:?}: expected 62 or 31"),
        }
    }

    pub fn from_env() -> Result<Self> {
        Self::parse(std::env::var(COOKIE_BITS_ENV).ok().as_deref())
    }
}

/// The filesystem's cookie function.
#[derive(Clone)]
pub struct CookieKey {
    hasher: SipHasher13,
    bits: CookieBits,
    /// Names the test hook maps to one cookie.
    #[cfg(test)]
    colliding: Vec<Vec<u8>>,
}

impl std::fmt::Debug for CookieKey {
    // The key is a secret: never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CookieKey")
            .field("bits", &self.bits)
            .finish_non_exhaustive()
    }
}

/// The cookie every name of [`CookieKey::colliding`] has.
#[cfg(test)]
pub const COLLIDING_COOKIE: u64 = 0x1234_5678;

impl CookieKey {
    /// The key of the filesystem described by `fsmeta`, with `e2e` its
    /// unlocked keyring when it has one.
    pub fn derive(e2e: Option<&SharedE2eKeys>, fsmeta: &FsMeta, bits: CookieBits) -> Result<Self> {
        let key: [u8; 32] = match e2e {
            Some(keys) => *keys.dek("readdir/cookie-v1"),
            None => {
                let Some(seed) = fsmeta.gossip_seed() else {
                    bail!(
                        "this filesystem predates `gossip_secret`, which its directory \
                         cookies are keyed by; recreate it (plan 40)"
                    );
                };
                blake3::derive_key("constellation readdir/cookie-v1", &seed)
            }
        };
        Ok(Self::from_key(&key, bits))
    }

    fn from_key(key: &[u8; 32], bits: CookieBits) -> Self {
        let k0 = u64::from_le_bytes(key[0..8].try_into().expect("8 bytes"));
        let k1 = u64::from_le_bytes(key[8..16].try_into().expect("8 bytes"));
        Self {
            hasher: SipHasher13::new_with_keys(k0, k1),
            bits,
            #[cfg(test)]
            colliding: Vec::new(),
        }
    }

    /// A key from two numbers (tests).
    #[cfg(test)]
    pub fn fixed(k0: u64, k1: u64) -> Self {
        Self {
            hasher: SipHasher13::new_with_keys(k0, k1),
            bits: CookieBits::Wide,
            colliding: Vec::new(),
        }
    }

    /// The same key at another width (tests).
    #[cfg(test)]
    pub fn with_bits(mut self, bits: CookieBits) -> Self {
        self.bits = bits;
        self
    }

    /// A key under which every one of `names` has [`COLLIDING_COOKIE`]
    /// (tests: two names of one cookie are otherwise a 2^-62 event).
    #[cfg(test)]
    pub fn colliding(names: &[&[u8]]) -> Self {
        let mut key = Self::fixed(1, 2);
        key.colliding = names.iter().map(|n| n.to_vec()).collect();
        key
    }

    pub fn bits(&self) -> CookieBits {
        self.bits
    }

    /// The cookie of the entry named `name` (the stored form's bytes).
    #[inline]
    pub fn cookie(&self, name: &[u8]) -> u64 {
        #[cfg(test)]
        if self.colliding.iter().any(|n| n == name) {
            return COLLIDING_COOKIE;
        }
        let hash = self.hasher.hash(name);
        match self.bits {
            CookieBits::Wide => (hash >> 2) + 3,
            CookieBits::Narrow => (hash % ((1 << 31) - 4)) + 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cookie_is_clear_of_the_dot_cookies_and_below_i64_max() {
        let key = CookieKey::fixed(7, 9);
        for i in 0..20_000u32 {
            let c = key.cookie(format!("name-{i}").as_bytes());
            assert!((3..=(1 << 62) + 2).contains(&c), "{c}");
        }
        assert!(key.cookie(b"") >= 3);
    }

    #[test]
    fn thirty_one_bit_cookies_stay_below_2_31() {
        let key = CookieKey::fixed(7, 9).with_bits(CookieBits::Narrow);
        let mut max = 0;
        for i in 0..200_000u32 {
            let c = key.cookie(format!("name-{i}").as_bytes());
            assert!((3..(1 << 31) - 1).contains(&c), "{c}");
            max = max.max(c);
        }
        assert!(max > 1 << 30, "the range is used: {max}");
    }

    #[test]
    fn the_key_decides_the_cookie() {
        let (a, b) = (CookieKey::fixed(1, 2), CookieKey::fixed(1, 3));
        let same = (0..1000u32)
            .filter(|i| {
                let n = format!("n{i}");
                a.cookie(n.as_bytes()) == b.cookie(n.as_bytes())
            })
            .count();
        assert_eq!(same, 0);
        assert_eq!(a.cookie(b"x"), CookieKey::fixed(1, 2).cookie(b"x"));
    }

    #[test]
    fn bits_parse() {
        assert_eq!(CookieBits::parse(None).unwrap(), CookieBits::Wide);
        assert_eq!(CookieBits::parse(Some("62")).unwrap(), CookieBits::Wide);
        assert_eq!(CookieBits::parse(Some(" 31 ")).unwrap(), CookieBits::Narrow);
        assert!(CookieBits::parse(Some("32")).is_err());
        assert!(CookieBits::parse(Some("wide")).is_err());
    }

    fn names_cookies(key: &CookieKey) -> Vec<u64> {
        (0..64)
            .map(|i| key.cookie(format!("n{i}").as_bytes()))
            .collect()
    }

    /// The key is derived from what the filesystem keeps: its seed (not
    /// E2E), or its master (E2E); a filesystem with neither has none.
    #[test]
    fn the_key_is_derived_from_the_seed_or_the_master() {
        let plain = FsMeta::new(1 << 20, "raw");
        let other = FsMeta::new(1 << 20, "raw");
        let a = CookieKey::derive(None, &plain, CookieBits::Wide).unwrap();
        assert_eq!(
            names_cookies(&a),
            names_cookies(&CookieKey::derive(None, &plain, CookieBits::Wide).unwrap()),
            "the same filesystem, the same cookies"
        );
        let b = CookieKey::derive(None, &other, CookieBits::Wide).unwrap();
        assert_ne!(
            names_cookies(&a),
            names_cookies(&b),
            "another seed, another key"
        );

        let mut seedless = FsMeta::new(1 << 20, "raw");
        seedless.gossip_secret = None;
        let err = CookieKey::derive(None, &seedless, CookieBits::Wide).unwrap_err();
        assert!(format!("{err:#}").contains("recreate"), "{err:#}");

        // E2E: the master decides; the (absent) seed does not matter.
        let block = constellation_store_s3::create_keyring_block("pw").unwrap();
        let keys = constellation_store_s3::unlock(&block, "pw").unwrap();
        let e = CookieKey::derive(Some(&keys), &seedless, CookieBits::Wide).unwrap();
        let again = constellation_store_s3::unlock(&block, "pw").unwrap();
        assert_eq!(
            names_cookies(&e),
            names_cookies(&CookieKey::derive(Some(&again), &plain, CookieBits::Wide).unwrap())
        );
        assert_ne!(names_cookies(&e), names_cookies(&a));
    }

    #[test]
    fn the_colliding_hook_maps_chosen_names_to_one_cookie() {
        let key = CookieKey::colliding(&[b"a", b"b"]);
        assert_eq!(key.cookie(b"a"), key.cookie(b"b"));
        assert_ne!(key.cookie(b"a"), key.cookie(b"c"));
    }
}
