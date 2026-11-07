//! A copy of `constellation_net::reconcile::Acc` (whose fields and
//! mutators are private), so candidates that cannot hold a `KeySet` can
//! still maintain per-shard accumulators and answer RBSR queries.
//! `tests::acc_matches_the_real_fingerprint` pins it to the real one.

use constellation_net::reconcile::{Fingerprint, Key};

fn mix(k: u64) -> u64 {
    let mut z = k.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Acc {
    pub count: u64,
    s1: u64,
    s2: u64,
}

impl Acc {
    pub fn add(&mut self, k: Key) {
        self.count += 1;
        self.s1 = self.s1.wrapping_add(k);
        self.s2 = self.s2.wrapping_add(mix(k));
    }
    pub fn sub(&mut self, k: Key) {
        self.count = self.count.wrapping_sub(1);
        self.s1 = self.s1.wrapping_sub(k);
        self.s2 = self.s2.wrapping_sub(mix(k));
    }
    pub fn merge(&mut self, o: &Acc) {
        self.count = self.count.wrapping_add(o.count);
        self.s1 = self.s1.wrapping_add(o.s1);
        self.s2 = self.s2.wrapping_add(o.s2);
    }
    pub fn unmerge(&mut self, o: &Acc) {
        self.count = self.count.wrapping_sub(o.count);
        self.s1 = self.s1.wrapping_sub(o.s1);
        self.s2 = self.s2.wrapping_sub(o.s2);
    }
    pub fn of(keys: &[Key]) -> Self {
        let mut a = Acc::default();
        for &k in keys {
            a.add(k);
        }
        a
    }
    pub fn fingerprint(&self) -> Fingerprint {
        let mut pre = [0u8; 24];
        pre[0..8].copy_from_slice(&self.count.to_le_bytes());
        pre[8..16].copy_from_slice(&self.s1.to_le_bytes());
        pre[16..24].copy_from_slice(&self.s2.to_le_bytes());
        blake3::hash(&pre).as_bytes()[0..16].try_into().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::Rng;
    use constellation_net::reconcile::KeySet;

    #[test]
    fn acc_matches_the_real_fingerprint() {
        let mut r = Rng::new(7);
        let keys: Vec<u64> = (0..1000).map(|_| r.next()).collect();
        let real = KeySet::from_keys(keys.iter().copied());
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(Acc::of(&sorted).fingerprint(), real.root_fingerprint());
        assert_eq!(Acc::of(&sorted).count as usize, real.len());
    }
}
