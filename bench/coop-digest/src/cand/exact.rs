//! C1: the status quo, the real `reconcile::Mirror`.

use crate::mirror::{LocationMirror, SeqState};
use constellation_net::reconcile::{
    DeltaOutcome, Fingerprint, Key, KeySet, Mirror, Range, Summary,
};

pub struct Exact {
    pub m: Mirror,
    dummy: SeqState,
}

impl Exact {
    pub fn from_keys(keys: &[Key]) -> Self {
        Self {
            m: Mirror {
                keys: KeySet::from_keys(keys.iter().copied()),
                ..Mirror::default()
            },
            dummy: SeqState::default(),
        }
    }
}

impl LocationMirror for Exact {
    fn label(&self) -> String {
        "C1 exact".into()
    }
    fn seq_view(&self) -> SeqState {
        SeqState {
            incarnation: self.m.incarnation,
            last_seq: self.m.last_seq,
        }
    }
    fn seq(&mut self) -> &mut SeqState {
        &mut self.dummy
    }
    fn len(&self) -> usize {
        self.m.keys.len()
    }
    fn root_fp(&self) -> Fingerprint {
        self.m.keys.root_fingerprint()
    }
    fn contains(&self, k: Key) -> bool {
        self.m.keys.contains(k)
    }
    fn add(&mut self, k: Key, _exact: bool) -> bool {
        self.m.keys.insert(k);
        true
    }
    fn del(&mut self, k: Key, _exact: bool) {
        self.m.keys.remove(k);
    }
    fn acc(&self, r: &Range) -> (Fingerprint, u64) {
        let a = self.m.keys.acc(r);
        (a.fingerprint(), a.count)
    }
    fn replace_range(&mut self, r: &Range, keys: &[Key]) {
        self.m.keys.replace_range(r, keys);
    }
    fn self_bytes(&self) -> usize {
        self.m.keys.approx_bytes()
    }
    fn keys(&self) -> Option<Vec<Key>> {
        Some(self.m.keys.iter().collect())
    }
    fn apply_delta(&mut self, d: &constellation_net::reconcile::Delta) -> DeltaOutcome {
        self.m.apply_delta(d)
    }
    fn matches(&mut self, s: &Summary) -> bool {
        self.m.matches(s)
    }
}
