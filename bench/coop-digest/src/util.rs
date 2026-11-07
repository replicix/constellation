//! Shared helpers: a seeded PRNG, a live-bytes counting allocator, RSS,
//! quantiles and key generators.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Wraps the system allocator and counts live requested bytes, so a
/// structure's heap cost is measured, not estimated from its capacity.
pub struct CountingAlloc;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method forwards to `System` unchanged and only adds
// relaxed counter updates.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Relaxed);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(l);
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l);
        LIVE.fetch_sub(l.size(), Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = System.realloc(p, l, new);
        if !q.is_null() {
            LIVE.fetch_add(new, Relaxed);
            LIVE.fetch_sub(l.size(), Relaxed);
        }
        q
    }
}

/// Live heap bytes (only meaningful when `CountingAlloc` is the global
/// allocator of the running binary).
pub fn live_bytes() -> usize {
    LIVE.load(Relaxed)
}

/// Resident set size in bytes from `/proc/self/statm` (4 KiB pages).
pub fn rss_bytes() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<usize>().ok())
        })
        .map_or(0, |p| p * 4096)
}

/// splitmix64 finalizer without the leading increment.
pub fn mix64(k: u64) -> u64 {
    let mut z = k;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic PRNG (splitmix64).
#[derive(Clone)]
pub struct Rng(pub u64);

#[allow(clippy::should_implement_trait)]
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix64(self.0)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next()) * u128::from(n.max(1))) >> 64) as u64
    }
    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Sorted, deduplicated uniform u64 keys for one peer. The same
/// `(seed, peer, n)` always gives the same set.
pub fn peer_keys(seed: u64, peer: u64, n: usize) -> Vec<u64> {
    let mut r = Rng::new(mix64(
        seed ^ peer.wrapping_mul(0xD6E8_FEB8_6659_FD93).wrapping_add(1),
    ));
    let mut v: Vec<u64> = (0..n).map(|_| r.next()).collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// Quantile (0..=1) of an unsorted slice; sorts a copy.
pub fn quantile(v: &[f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let i = ((s.len() - 1) as f64 * q).round() as usize;
    s[i]
}

/// `"median (min..max)"` of repeated runs.
pub fn spread(v: &[f64]) -> String {
    let mn = v.iter().cloned().fold(f64::INFINITY, f64::min);
    let mx = v.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    format!("{:.1} ({:.1}..{:.1})", quantile(v, 0.5), mn, mx)
}

/// Minimal `--key value` argument lookup.
pub struct Args(pub Vec<String>);

impl Args {
    pub fn get(&self, key: &str) -> Option<String> {
        let k = format!("--{key}");
        self.0
            .iter()
            .position(|a| *a == k)
            .and_then(|i| self.0.get(i + 1).cloned())
    }
    pub fn str(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or_else(|| default.to_string())
    }
    pub fn num<T: std::str::FromStr>(&self, key: &str, default: T) -> T {
        self.get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }
    pub fn flag(&self, key: &str) -> bool {
        let k = format!("--{key}");
        self.0.contains(&k)
    }
}

/// `1.23 MiB` style size.
pub fn human(b: f64) -> String {
    if b >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GiB", b / (1024.0 * 1024.0 * 1024.0))
    } else if b >= 1024.0 * 1024.0 {
        format!("{:.2} MiB", b / (1024.0 * 1024.0))
    } else if b >= 1024.0 {
        format!("{:.1} KiB", b / 1024.0)
    } else {
        format!("{b:.0} B")
    }
}
