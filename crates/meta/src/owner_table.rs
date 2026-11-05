//! The sequencer's owner table ([`OwnerTable`]: the grant records
//! `LockTables` keeps for the locks it owns) and its memory of lapsed
//! records ([`LapsedMemory`]), each with its indexes behind private fields.
//!
//! Plan 41 item 7: closing 10,000 locked files of the sequencer's own
//! grants admitted their late renewals at O(n) each (`admit_late` scanned
//! every record for a conflicting one, dropped the node's older record
//! with a `retain` over all of them and looked the id up in the lapsed
//! memory by scanning it), so one renewal step took up to 58 s and more
//! grants lapsed behind it. Every lookup by inode is now O(records on the
//! inode), the expiry check O(expired), the lapsed memory O(log n) per
//! change.
//!
//! The records feed the checks that stop two holders (`conflicting`,
//! `admit_late`'s `beside`, `install_if_consistent`): an index that
//! disagreed with the records would grant a conflicting lock. So the
//! records are reachable only through these types — every insert, removal
//! and change goes through a method that keeps the indexes — and debug and
//! simulation builds (`feature = "test-util"`) and unit tests rebuild the indexes after
//! every change and compare (in full up to [`CHECK_FULL_UP_TO`] records;
//! past that, the cheap invariants only), and compare each lookup by inode
//! with a scan.

use crate::locks::{Grant, GrantId};
use constellation_types::clock::WaitMs;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Up to this many records, the debug cross-check rebuilds the indexes in
/// full after every change and scans for every lookup by inode; past it,
/// it checks only the counts and each index change (a 10,000-record unit
/// test would otherwise be O(n²)).
#[cfg(any(test, debug_assertions, feature = "test-util"))]
const CHECK_FULL_UP_TO: usize = 512;

/// The grant records, by id, with an index by inode and one by end.
#[derive(Default)]
pub(crate) struct OwnerTable {
    by_id: BTreeMap<GrantId, Grant>,
    /// `ino` → the ids of its records (id order: the order a scan of
    /// `by_id` filtered by inode gave, so every caller sees the same order
    /// as before the index).
    by_ino: BTreeMap<u64, BTreeSet<GrantId>>,
    /// `(until, id)` of every record: the expired ones are a prefix.
    by_until: BTreeSet<(WaitMs, GrantId)>,
    /// Ids in `by_ino`, summed (the cheap check: equals `by_id.len()`).
    indexed: usize,
}

impl OwnerTable {
    pub(crate) fn len(&self) -> usize {
        self.by_id.len()
    }

    pub(crate) fn get(&self, id: &GrantId) -> Option<&Grant> {
        self.by_id.get(id)
    }

    pub(crate) fn contains_key(&self, id: &GrantId) -> bool {
        self.by_id.contains_key(id)
    }

    /// Every record, in id order.
    pub(crate) fn values(&self) -> impl Iterator<Item = &Grant> {
        self.by_id.values()
    }

    /// The records on `ino`, in id order.
    pub(crate) fn on_ino(&self, ino: u64) -> impl Iterator<Item = &Grant> {
        #[cfg(any(test, debug_assertions, feature = "test-util"))]
        if self.by_id.len() <= CHECK_FULL_UP_TO {
            let scan: Vec<GrantId> = self
                .by_id
                .values()
                .filter(|e| e.ino == ino)
                .map(|e| e.id)
                .collect();
            let indexed: Vec<GrantId> = self
                .by_ino
                .get(&ino)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default();
            assert_eq!(
                indexed, scan,
                "owner table: inode index of {ino} disagrees with a scan"
            );
        }
        self.by_ino
            .get(&ino)
            .into_iter()
            .flatten()
            .map(|id| self.by_id.get(id).expect("indexed id has a record"))
    }

    /// Insert (or replace) the record `g.id`.
    pub(crate) fn insert(&mut self, g: Grant) -> Option<Grant> {
        let old = self.by_id.insert(g.id, g);
        if let Some(o) = &old {
            self.unindex(o);
        }
        self.index(&g);
        self.check();
        old
    }

    pub(crate) fn remove(&mut self, id: &GrantId) -> Option<Grant> {
        let gone = self.by_id.remove(id);
        if let Some(e) = &gone {
            self.unindex(e);
        }
        self.check();
        gone
    }

    /// Change the record `id` in place (`None`: no such record). `f` must
    /// not change the id; a changed inode or end is re-indexed.
    pub(crate) fn update<R>(&mut self, id: &GrantId, f: impl FnOnce(&mut Grant) -> R) -> Option<R> {
        let e = self.by_id.get_mut(id)?;
        let before = *e;
        let out = f(e);
        let after = *e;
        assert_eq!(
            after.id, before.id,
            "owner table: an update changed a record's id"
        );
        if after.ino != before.ino || after.until != before.until {
            self.unindex(&before);
            self.index(&after);
        }
        self.check();
        Some(out)
    }

    /// Remove the records on `ino` for which `f` holds; returned in id order.
    pub(crate) fn remove_on_ino_where(
        &mut self,
        ino: u64,
        mut f: impl FnMut(&Grant) -> bool,
    ) -> Vec<Grant> {
        let ids: Vec<GrantId> = self.on_ino(ino).filter(|e| f(e)).map(|e| e.id).collect();
        ids.iter().filter_map(|id| self.remove(id)).collect()
    }

    /// Remove the records for which `f` holds (a scan of every record);
    /// returned in id order.
    pub(crate) fn remove_where(&mut self, mut f: impl FnMut(&Grant) -> bool) -> Vec<Grant> {
        let ids: Vec<GrantId> = self.by_id.values().filter(|e| f(e)).map(|e| e.id).collect();
        ids.iter().filter_map(|id| self.remove(id)).collect()
    }

    /// Whether a record ends at or before `now`.
    pub(crate) fn any_expired(&self, now: WaitMs) -> bool {
        self.by_until
            .first()
            .is_some_and(|(until, _)| *until <= now)
    }

    /// The records that end at or before `now`, in id order (what a scan
    /// gave).
    pub(crate) fn expired(&self, now: WaitMs) -> Vec<Grant> {
        let mut ids: Vec<GrantId> = self
            .by_until
            .iter()
            .take_while(|(until, _)| *until <= now)
            .map(|(_, id)| *id)
            .collect();
        ids.sort_unstable();
        ids.iter()
            .map(|id| *self.by_id.get(id).expect("indexed id has a record"))
            .collect()
    }

    pub(crate) fn clear(&mut self) {
        self.by_id.clear();
        self.by_ino.clear();
        self.by_until.clear();
        self.indexed = 0;
        self.check();
    }

    fn index(&mut self, e: &Grant) {
        let fresh = self.by_ino.entry(e.ino).or_default().insert(e.id);
        assert!(fresh, "owner table: {:?} indexed twice on its inode", e.id);
        let fresh = self.by_until.insert((e.until, e.id));
        assert!(fresh, "owner table: {:?} indexed twice by its end", e.id);
        self.indexed += 1;
    }

    fn unindex(&mut self, e: &Grant) {
        let mut found = false;
        if let Some(s) = self.by_ino.get_mut(&e.ino) {
            found = s.remove(&e.id);
            if s.is_empty() {
                self.by_ino.remove(&e.ino);
            }
        }
        assert!(
            found,
            "owner table: {:?} missing from its inode's index",
            e.id
        );
        let found = self.by_until.remove(&(e.until, e.id));
        assert!(
            found,
            "owner table: {:?} missing from the index by end",
            e.id
        );
        self.indexed -= 1;
    }

    /// Debug and simulation builds: the indexes equal a rebuild from the
    /// records (in full up to [`CHECK_FULL_UP_TO`] records).
    #[cfg(any(test, debug_assertions, feature = "test-util"))]
    fn check(&self) {
        assert_eq!(
            self.indexed,
            self.by_id.len(),
            "owner table: inode index size"
        );
        assert_eq!(
            self.by_until.len(),
            self.by_id.len(),
            "owner table: end index size"
        );
        if self.by_id.len() > CHECK_FULL_UP_TO {
            return;
        }
        let mut by_ino: BTreeMap<u64, BTreeSet<GrantId>> = BTreeMap::new();
        let mut by_until = BTreeSet::new();
        for (id, e) in &self.by_id {
            assert_eq!(*id, e.id, "owner table: a record under another id");
            by_ino.entry(e.ino).or_default().insert(e.id);
            by_until.insert((e.until, e.id));
        }
        assert_eq!(
            by_ino, self.by_ino,
            "owner table: inode index disagrees with a rebuild"
        );
        assert_eq!(
            by_until, self.by_until,
            "owner table: end index disagrees with a rebuild"
        );
    }

    #[cfg(not(any(test, debug_assertions, feature = "test-util")))]
    #[inline(always)]
    fn check(&self) {}
}

/// The memory of lapsed records, by inode, with an id → inode index and
/// an eviction order (oldest first) whose stale entries — ids forgotten
/// or remembered again since — are skipped when popped and compacted away
/// in bulk, so forgetting one is O(log n), not a search of the order.
#[derive(Default)]
pub(crate) struct LapsedMemory {
    /// Per inode, in the order remembered (what the callers iterate).
    by_ino: BTreeMap<u64, Vec<Grant>>,
    /// id → (inode, the stamp of its entry in `order`).
    at: BTreeMap<GrantId, (u64, u64)>,
    /// `(stamp, id)`, oldest first; an entry whose stamp is not `at[id]`'s
    /// is stale.
    order: VecDeque<(u64, GrantId)>,
    stamp: u64,
}

impl LapsedMemory {
    /// How many are remembered.
    pub(crate) fn len(&self) -> usize {
        self.at.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.at.is_empty()
    }

    pub(crate) fn contains(&self, id: &GrantId) -> bool {
        self.at.contains_key(id)
    }

    pub(crate) fn get(&self, id: &GrantId) -> Option<&Grant> {
        let (ino, _) = self.at.get(id)?;
        self.by_ino.get(ino)?.iter().find(|e| e.id == *id)
    }

    /// The memories of `ino`, oldest first.
    pub(crate) fn on_ino(&self, ino: u64) -> &[Grant] {
        self.by_ino.get(&ino).map_or(&[], |v| v.as_slice())
    }

    /// Every memory, by inode then age.
    pub(crate) fn values(&self) -> impl Iterator<Item = &Grant> {
        self.by_ino.values().flatten()
    }

    /// Remember `e` (an earlier memory of its id goes first); past `cap`
    /// memories the oldest go.
    pub(crate) fn remember(&mut self, e: Grant, cap: usize) {
        self.forget(&e.id);
        self.stamp += 1;
        self.by_ino.entry(e.ino).or_default().push(e);
        self.at.insert(e.id, (e.ino, self.stamp));
        self.order.push_back((self.stamp, e.id));
        while self.at.len() > cap {
            let Some((stamp, id)) = self.order.pop_front() else {
                break;
            };
            if self.at.get(&id).is_some_and(|(_, s)| *s == stamp) {
                self.forget(&id);
            }
        }
        // Stale entries are at most as many as the live ones (plus slack):
        // compacting costs O(n) once per O(n) forgets.
        if self.order.len() > 2 * self.at.len() + 64 {
            let at = &self.at;
            self.order
                .retain(|(stamp, id)| at.get(id).is_some_and(|(_, s)| s == stamp));
        }
        self.check();
    }

    /// `id`'s memory goes.
    pub(crate) fn forget(&mut self, id: &GrantId) -> Option<Grant> {
        let (ino, _) = self.at.remove(id)?;
        let v = self
            .by_ino
            .get_mut(&ino)
            .expect("indexed inode has memories");
        let i = v
            .iter()
            .position(|e| e.id == *id)
            .expect("indexed id is remembered");
        let g = v.remove(i);
        if v.is_empty() {
            self.by_ino.remove(&ino);
        }
        self.check();
        Some(g)
    }

    /// Forget every memory for which `f` holds.
    pub(crate) fn forget_where(&mut self, mut f: impl FnMut(&Grant) -> bool) {
        let gone: Vec<GrantId> = self.values().filter(|e| f(e)).map(|e| e.id).collect();
        for id in gone {
            self.forget(&id);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.by_ino.clear();
        self.at.clear();
        self.order.clear();
        self.check();
    }

    #[cfg(any(test, debug_assertions, feature = "test-util"))]
    fn check(&self) {
        if self.at.len() > CHECK_FULL_UP_TO {
            return;
        }
        let mut n = 0;
        for (ino, v) in &self.by_ino {
            assert!(!v.is_empty(), "lapsed memory: empty inode entry {ino}");
            for e in v {
                assert_eq!(e.ino, *ino, "lapsed memory: a memory under another inode");
                assert_eq!(
                    self.at.get(&e.id).map(|(i, _)| *i),
                    Some(*ino),
                    "lapsed memory: id index disagrees for {:?}",
                    e.id
                );
                n += 1;
            }
        }
        assert_eq!(n, self.at.len(), "lapsed memory: id index size");
        let live: BTreeSet<(u64, GrantId)> = self.at.iter().map(|(id, (_, s))| (*s, *id)).collect();
        let queued: BTreeSet<(u64, GrantId)> = self
            .order
            .iter()
            .copied()
            .filter(|e| live.contains(e))
            .collect();
        assert_eq!(
            live, queued,
            "lapsed memory: a memory missing from the eviction order"
        );
    }

    #[cfg(not(any(test, debug_assertions, feature = "test-util")))]
    #[inline(always)]
    fn check(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::locks::LockMode;

    fn rec(seq: u64, node: u64, ino: u64, until: i64) -> Grant {
        Grant {
            id: GrantId { node: 1, seq },
            node,
            ino,
            mode: LockMode::Shared,
            until: WaitMs(until),
            recalled: false,
            gen: 0,
            confirmed: Grant::MINTED,
            settled: true,
        }
    }

    /// The naive model: a plain map scanned for every question.
    fn model_on_ino(m: &BTreeMap<GrantId, Grant>, ino: u64) -> Vec<Grant> {
        m.values().filter(|e| e.ino == ino).copied().collect()
    }

    fn assert_same(t: &OwnerTable, m: &BTreeMap<GrantId, Grant>, now: i64) {
        assert_eq!(t.len(), m.len());
        assert_eq!(
            t.values().copied().collect::<Vec<_>>(),
            m.values().copied().collect::<Vec<_>>()
        );
        for ino in 0..8 {
            assert_eq!(
                t.on_ino(ino).copied().collect::<Vec<_>>(),
                model_on_ino(m, ino)
            );
        }
        let exp: Vec<Grant> = m.values().filter(|e| e.until.0 <= now).copied().collect();
        assert_eq!(t.expired(WaitMs(now)), exp);
        assert_eq!(t.any_expired(WaitMs(now)), !exp.is_empty());
    }

    #[test]
    fn insert_remove_move_and_expire_keep_the_indexes_exact() {
        let mut t = OwnerTable::default();
        t.insert(rec(1, 2, 7, 100));
        t.insert(rec(2, 3, 7, 50));
        t.insert(rec(3, 2, 8, 200));
        assert_eq!(
            t.on_ino(7).map(|e| e.id.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        // A replacement under the same id moves it to another inode.
        t.insert(rec(2, 3, 8, 50));
        assert_eq!(t.on_ino(7).map(|e| e.id.seq).collect::<Vec<_>>(), vec![1]);
        assert_eq!(
            t.on_ino(8).map(|e| e.id.seq).collect::<Vec<_>>(),
            vec![2, 3]
        );
        // An update that moves the end re-indexes it.
        assert!(t.any_expired(WaitMs(50)));
        t.update(&GrantId { node: 1, seq: 2 }, |e| e.until = WaitMs(300));
        assert!(!t.any_expired(WaitMs(99)));
        assert_eq!(t.expired(WaitMs(100)).len(), 1);
        // An update that moves the inode re-indexes it.
        t.update(&GrantId { node: 1, seq: 3 }, |e| e.ino = 9);
        assert_eq!(t.on_ino(9).count(), 1);
        assert_eq!(t.on_ino(8).count(), 1);
        let gone = t.remove_on_ino_where(8, |e| e.node == 3);
        assert_eq!(gone.len(), 1);
        assert_eq!(t.on_ino(8).count(), 0);
        assert!(t.remove(&GrantId { node: 1, seq: 1 }).is_some());
        assert!(t.remove(&GrantId { node: 1, seq: 1 }).is_none());
        t.remove_where(|_| true);
        assert!(t.len() == 0 && !t.any_expired(WaitMs(i64::MAX)));
        t.insert(rec(4, 1, 1, 1));
        t.clear();
        assert!(t.len() == 0 && t.on_ino(1).next().is_none());
    }

    #[test]
    #[should_panic(expected = "changed a record's id")]
    fn an_update_cannot_change_the_id() {
        let mut t = OwnerTable::default();
        t.insert(rec(1, 2, 7, 100));
        t.update(&GrantId { node: 1, seq: 1 }, |e| e.id.seq = 5);
    }

    /// Small xorshift: the test needs no crate, and a seed replays.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    #[test]
    fn random_operations_agree_with_a_naive_model() {
        for seed in 1..=40u64 {
            let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let mut t = OwnerTable::default();
            let mut m: BTreeMap<GrantId, Grant> = BTreeMap::new();
            for step in 0..600 {
                let now = r.below(120) as i64;
                let seq = r.below(48);
                let id = GrantId { node: 1, seq };
                match r.below(7) {
                    0 | 1 => {
                        let g = rec(seq, r.below(4), r.below(8), r.below(120) as i64);
                        assert_eq!(t.insert(g), m.insert(id, g), "seed {seed} step {step}");
                    }
                    2 => assert_eq!(t.remove(&id), m.remove(&id), "seed {seed} step {step}"),
                    3 => {
                        let (ino, until) = (r.below(8), r.below(120) as i64);
                        let f = |e: &mut Grant| {
                            e.until = e.until.max(WaitMs(until));
                            if until % 3 == 0 {
                                e.ino = ino;
                            }
                            e.recalled = !e.recalled;
                        };
                        let a = t.update(&id, f);
                        let b = m.get_mut(&id).map(f);
                        assert_eq!(a, b);
                    }
                    4 => {
                        let (ino, node) = (r.below(8), r.below(4));
                        let got = t.remove_on_ino_where(ino, |e| e.node == node);
                        let want: Vec<Grant> = model_on_ino(&m, ino)
                            .into_iter()
                            .filter(|e| e.node == node)
                            .collect();
                        for w in &want {
                            m.remove(&w.id);
                        }
                        assert_eq!(got, want, "seed {seed} step {step}");
                    }
                    5 => {
                        // Drop the expired ones, as `drop_expired` does.
                        let got: Vec<Grant> = t.expired(WaitMs(now));
                        for e in &got {
                            t.remove(&e.id);
                        }
                        let want: Vec<Grant> =
                            m.values().filter(|e| e.until.0 <= now).copied().collect();
                        for w in &want {
                            m.remove(&w.id);
                        }
                        assert_eq!(got, want, "seed {seed} step {step}");
                    }
                    _ => {
                        let gen = r.below(4);
                        let got = t.remove_where(|e| e.node == gen);
                        let want: Vec<Grant> =
                            m.values().filter(|e| e.node == gen).copied().collect();
                        for w in &want {
                            m.remove(&w.id);
                        }
                        assert_eq!(got, want);
                    }
                }
                assert_same(&t, &m, now);
            }
        }
    }

    #[test]
    fn the_lapsed_memory_forgets_evicts_and_compacts_by_index() {
        let mut l = LapsedMemory::default();
        for seq in 0..10 {
            l.remember(rec(seq, 2, seq % 3, 0), 4);
        }
        // The four newest are kept, oldest evicted first.
        assert_eq!(l.len(), 4);
        let kept: BTreeSet<u64> = l.values().map(|e| e.id.seq).collect();
        assert_eq!(kept, [6, 7, 8, 9].into_iter().collect());
        // Remembered again: it is the newest now, its old place in the
        // order is stale and must not evict it.
        l.remember(rec(6, 2, 0, 5), 4);
        l.remember(rec(10, 2, 1, 0), 4);
        assert!(l.contains(&GrantId { node: 1, seq: 6 }));
        assert!(!l.contains(&GrantId { node: 1, seq: 7 }));
        assert_eq!(
            l.get(&GrantId { node: 1, seq: 6 }).unwrap().until,
            WaitMs(5)
        );
        assert!(l.forget(&GrantId { node: 1, seq: 9 }).is_some());
        assert!(l.forget(&GrantId { node: 1, seq: 9 }).is_none());
        l.forget_where(|e| e.ino == 1);
        assert!(l.on_ino(1).is_empty());
        // Many forgets and re-remembers keep the order bounded.
        for i in 0..10_000u64 {
            l.remember(rec(100 + i % 3, 2, i % 5, 0), 4);
        }
        assert!(l.order.len() <= 2 * l.len() + 64 + 1);
        l.clear();
        assert!(l.is_empty() && l.values().next().is_none());
    }

    #[test]
    fn random_lapsed_operations_agree_with_a_naive_model() {
        for seed in 1..=40u64 {
            let mut r = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
            let mut l = LapsedMemory::default();
            // The model: the order remembered, oldest first (what the
            // memory was before its index: a list searched each time).
            let mut m: Vec<Grant> = Vec::new();
            let cap = 1 + r.below(16) as usize;
            for _ in 0..800 {
                let seq = r.below(40);
                let id = GrantId { node: 1, seq };
                match r.below(4) {
                    0 | 1 => {
                        let g = rec(seq, r.below(4), r.below(6), 0);
                        l.remember(g, cap);
                        m.retain(|e| e.id != id);
                        m.push(g);
                        while m.len() > cap {
                            m.remove(0);
                        }
                    }
                    2 => {
                        let want = m.iter().position(|e| e.id == id).map(|i| m.remove(i));
                        assert_eq!(l.forget(&id), want);
                    }
                    _ => {
                        let ino = r.below(6);
                        l.forget_where(|e| e.ino == ino);
                        m.retain(|e| e.ino != ino);
                    }
                }
                assert_eq!(l.len(), m.len());
                for ino in 0..6 {
                    let want: Vec<Grant> = m.iter().filter(|e| e.ino == ino).copied().collect();
                    assert_eq!(l.on_ino(ino), want.as_slice());
                }
                for seq in 0..40 {
                    let id = GrantId { node: 1, seq };
                    assert_eq!(l.get(&id).copied(), m.iter().find(|e| e.id == id).copied());
                }
            }
        }
    }
}
