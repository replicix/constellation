//! Owner side: a copy of `LocalSet::absorb` from
//! `crates/engine/src/coop/exact.rs` at the commit named in RESULTS.md
//! (`LocalSet` is `pub(super)` in the engine, and P1 changes no product
//! code). The only differences: events arrive as `(key, present)`
//! pairs instead of `DigestChange` hashes, and the recent-removals
//! bookkeeping (which no candidate touches) is omitted.

use crate::util::Rng;
use constellation_net::message::{wire_len, Payload};
use constellation_net::reconcile::{encode_keys, Delta, Key, KeySet, Summary, MAX_DELTA_KEYS};
use std::collections::{HashMap, HashSet};

/// `MAX_DELTAS_PER_TICK` in `exact.rs`.
pub const MAX_DELTAS_PER_TICK: usize = 8;

pub struct Publisher {
    pub keys: KeySet,
    pub seq: u64,
    pub incarnation: u64,
}

#[derive(Default)]
pub struct Absorbed {
    pub deltas: Vec<Delta>,
    pub silent: bool,
}

impl Publisher {
    pub fn new(keys: KeySet, incarnation: u64) -> Self {
        Self {
            keys,
            seq: 0,
            incarnation,
        }
    }

    pub fn summary(&self) -> Summary {
        Summary {
            incarnation: self.incarnation,
            seq: self.seq,
            root: self.keys.root_fingerprint(),
            count: self.keys.len() as u64,
        }
    }

    /// Fold one journal drain into the published set (`rebuild`: the
    /// events are the cache's complete new contents).
    pub fn absorb(&mut self, events: &[(Key, bool)], rebuild: bool, emit: bool) -> Absorbed {
        let mut touched: HashMap<Key, bool> = HashMap::new();
        if rebuild {
            let fresh: HashSet<Key> = events.iter().filter(|e| e.1).map(|e| e.0).collect();
            for k in self.keys.iter() {
                if !fresh.contains(&k) {
                    touched.insert(k, false);
                }
            }
            for k in fresh {
                touched.insert(k, true);
            }
        } else {
            for &(k, want) in events {
                touched.insert(k, want);
            }
        }
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for (k, want) in touched {
            match (want, self.keys.contains(k)) {
                (true, false) => adds.push(k),
                (false, true) => removes.push(k),
                _ => {}
            }
        }
        if adds.is_empty() && removes.is_empty() {
            return Absorbed::default();
        }
        adds.sort_unstable();
        removes.sort_unstable();
        let total = adds.len() + removes.len();
        if !emit || total > MAX_DELTA_KEYS * MAX_DELTAS_PER_TICK {
            for &k in &removes {
                self.keys.remove(k);
            }
            for &k in &adds {
                self.keys.insert(k);
            }
            self.seq += 1;
            return Absorbed {
                deltas: Vec::new(),
                silent: emit,
            };
        }
        let ops: Vec<(Key, bool)> = removes
            .iter()
            .map(|&k| (k, false))
            .chain(adds.iter().map(|&k| (k, true)))
            .collect();
        let mut deltas = Vec::new();
        for chunk in ops.chunks(MAX_DELTA_KEYS) {
            let base = self.keys.root_fingerprint();
            let mut a = Vec::new();
            let mut r = Vec::new();
            for &(k, add) in chunk {
                if add {
                    self.keys.insert(k);
                    a.push(k);
                } else {
                    self.keys.remove(k);
                    r.push(k);
                }
            }
            self.seq += 1;
            deltas.push(Delta {
                incarnation: self.incarnation,
                seq: self.seq,
                base,
                root: self.keys.root_fingerprint(),
                adds: encode_keys(0, &a),
                removes: encode_keys(0, &r),
            });
        }
        Absorbed {
            deltas,
            silent: false,
        }
    }
}

/// Wire bytes of one gossiped delta (signed frame).
pub fn delta_wire(d: &Delta) -> usize {
    wire_len(&Payload::CacheSetDelta {
        node_id: 1,
        delta: d.clone(),
    })
}

/// Wire bytes of one summary heartbeat.
pub fn summary_wire(s: &Summary) -> usize {
    wire_len(&Payload::CacheSummary {
        node_id: 1,
        summary: *s,
    })
}

/// Synthetic churn: each tick removes random current keys and adds
/// fresh random ones; half and half (the default) keeps the set level.
pub struct Churn {
    pub rng: Rng,
    pub cur: Vec<Key>,
    /// Fraction of events that are additions (0.5 keeps the set level).
    pub add_frac: f64,
}

impl Churn {
    pub fn new(seed: u64, keys: &KeySet) -> Self {
        Self {
            rng: Rng::new(seed),
            cur: keys.iter().collect(),
            add_frac: 0.5,
        }
    }

    /// `changes` events: removals of present keys and additions of fresh
    /// keys, `add_frac` of them additions.
    pub fn tick(&mut self, changes: usize) -> Vec<(Key, bool)> {
        let mut ev = Vec::with_capacity(changes);
        for _ in 0..changes {
            if self.rng.unit() >= self.add_frac && !self.cur.is_empty() {
                let j = self.rng.below(self.cur.len() as u64) as usize;
                let k = self.cur.swap_remove(j);
                ev.push((k, false));
            } else {
                let k = self.rng.next();
                self.cur.push(k);
                ev.push((k, true));
            }
        }
        ev
    }

    /// Changes per 250 ms tick for a churn of `pct` percent of `n` per
    /// minute (fractional carried by the caller via `carry`).
    pub fn per_tick(n: usize, pct: f64, carry: &mut f64) -> usize {
        *carry += n as f64 * pct / 100.0 / 240.0;
        let c = carry.floor();
        *carry -= c;
        c as usize
    }
}
