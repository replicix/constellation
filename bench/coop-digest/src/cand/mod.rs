//! Candidate receiver structures behind `LocationMirror`.

pub mod bloomc;
pub mod cuckoo;
pub mod ef;
pub mod exact;
pub mod fuse;
pub mod hybrid;
pub mod quotient;
pub mod suffix;

/// `reconcile.rs`'s shard sizing, parameterised by the target keys per
/// shard: smallest bit count with `2^bits * target >= n`, capped at 16.
pub fn ideal_bits(n: usize, target: usize) -> u8 {
    let mut b = 0u8;
    while b < 16 && (1usize << b) * target < n {
        b += 1;
    }
    b
}

/// Shard index of `k` at `bits` prefix bits.
#[inline]
pub fn shard_idx(k: u64, bits: u8) -> usize {
    if bits == 0 {
        0
    } else {
        (k >> (64 - u32::from(bits))) as usize
    }
}

/// Inclusive shard span covered by `r`.
pub fn span(r: &constellation_net::reconcile::Range, bits: u8) -> (usize, usize) {
    (shard_idx(r.lo(), bits), shard_idx(r.hi(), bits))
}

/// Which candidate to build, with its knobs.
#[derive(Debug, Clone)]
pub enum Spec {
    Exact,
    Ef,
    /// Suffix mirror: suffix bytes, target keys per shard.
    Suffix(usize, usize),
    /// Cuckoo: fingerprint bits, target keys per shard.
    Cuckoo(u32, usize),
    /// Fuse overlay: target keys per bucket.
    Fuse(usize),
    /// Quotient filter: log2(1/error), target keys per shard.
    Quotient(u32, usize),
    Bloom,
    /// Hybrid: cuckoo fingerprint bits, target keys per shard.
    Hybrid(u32, usize),
}

impl Spec {
    pub fn parse(s: &str) -> Option<Spec> {
        let mut it = s.split(':');
        let name = it.next()?;
        let a: Option<usize> = it.next().and_then(|v| v.parse().ok());
        let b: Option<usize> = it.next().and_then(|v| v.parse().ok());
        Some(match name {
            "c1" | "exact" => Spec::Exact,
            "c6a" | "ef" => Spec::Ef,
            "c6b" | "suffix" => Spec::Suffix(a.unwrap_or(4), b.unwrap_or(64)),
            "c3" | "cuckoo" => Spec::Cuckoo(a.unwrap_or(16) as u32, b.unwrap_or(256)),
            "c5" | "fuse" => Spec::Fuse(a.unwrap_or(4096)),
            "c4" | "quotient" => Spec::Quotient(a.unwrap_or(14) as u32, b.unwrap_or(256)),
            "bloom" => Spec::Bloom,
            "c8a" | "hybrid" => Spec::Hybrid(a.unwrap_or(16) as u32, b.unwrap_or(256)),
            _ => return None,
        })
    }

    /// Build a mirror holding `keys` (sorted, unique) in bulk. `owner_n`
    /// is the advertised owner count the structure is sized from.
    pub fn build(&self, keys: &[u64], owner_n: usize) -> Box<dyn crate::mirror::LocationMirror> {
        use crate::mirror::LocationMirror;
        let b: Box<dyn LocationMirror> = match self {
            Spec::Exact => Box::new(exact::Exact::from_keys(keys)),
            Spec::Ef => Box::new(ef::EfSet::from_sorted(keys, 64)),
            Spec::Suffix(w, t) => Box::new(suffix::SuffixSet::from_sorted(keys, owner_n, *w, *t)),
            Spec::Cuckoo(f, t) => Box::new(cuckoo::CuckooSet::from_sorted(keys, owner_n, *f, *t)),
            Spec::Fuse(t) => Box::new(fuse::FuseSet::from_sorted(keys, owner_n, *t)),
            Spec::Quotient(e, t) => {
                Box::new(quotient::QuotientSet::from_sorted(keys, owner_n, *e, *t))
            }
            Spec::Bloom => Box::new(bloomc::BloomMirror::from_keys(keys)),
            Spec::Hybrid(f, t) => Box::new(hybrid::Hybrid::from_sorted(keys, owner_n, *f, *t)),
        };
        b
    }

    /// An empty mirror sized for an owner of `owner_n` keys (initial
    /// sync).
    pub fn empty(&self, owner_n: usize) -> Box<dyn crate::mirror::LocationMirror> {
        self.build(&[], owner_n)
    }
}
