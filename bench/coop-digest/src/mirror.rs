//! The receiver-side interface every candidate implements, and the
//! generic RBSR initiator that drives any of them against the real
//! owner-side `reconcile::respond`.

use crate::acc::Acc;
use constellation_net::message::{wire_len, Payload};
use constellation_net::reconcile::{
    decode_keys, Answer, Delta, DeltaOutcome, Fingerprint, Key, KeySet, Query, Range, Reply,
    Summary, FANOUT_BITS, MAX_DELTA_KEYS, MAX_QUERIES_PER_REQUEST, MAX_ROUNDS, REPLY_BUDGET,
};
use std::collections::HashMap;
use std::time::Instant;

/// Incarnation / sequence bookkeeping shared by all mirrors
/// (`reconcile::Mirror` keeps the same two fields).
#[derive(Debug, Default, Clone, Copy)]
pub struct SeqState {
    pub incarnation: Option<u64>,
    pub last_seq: u64,
}

/// One peer's location structure as the receiver holds it.
pub trait LocationMirror {
    fn label(&self) -> String;
    fn seq(&mut self) -> &mut SeqState;
    /// Keys the structure believes the owner holds.
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Fingerprint of the believed set (from exact accumulators).
    fn root_fp(&self) -> Fingerprint;
    fn contains(&self, k: Key) -> bool;
    /// Apply one added key. `exact`: the mirror is on the delta's base,
    /// so the change is certain and needs no membership check. Returns
    /// false when the structure could not store it (a full filter).
    fn add(&mut self, k: Key, exact: bool) -> bool;
    fn del(&mut self, k: Key, exact: bool);
    /// Finest prefix depth at which `acc` can be answered; `None` means
    /// any depth (the mirror holds exact keys).
    fn granularity(&self) -> Option<u8> {
        None
    }
    /// Fingerprint and count of the believed keys in `r`
    /// (`r.bits <= granularity`).
    fn acc(&self, r: &Range) -> (Fingerprint, u64);
    /// Make the believed set inside `r` exactly `keys`.
    fn replace_range(&mut self, r: &Range, keys: &[Key]);
    /// Heap bytes the structure reports for itself (the allocator
    /// measurement is the figure of record).
    fn self_bytes(&self) -> usize;
    /// Ranges the structure wants refetched from the owner (a shard whose
    /// filter filled up).
    fn dirty_ranges(&mut self) -> Vec<Range> {
        Vec::new()
    }
    /// Can this structure be repaired by a reconciliation session?
    fn repairs(&self) -> bool {
        true
    }
    fn seq_view(&self) -> SeqState;
    /// Called once per digest interval with the owner's current set;
    /// only Bloom uses it (its next snapshot).
    fn on_interval(&mut self, _owner: &KeySet) {}
    /// Enumerate the believed keys, when the structure can.
    fn keys(&self) -> Option<Vec<Key>> {
        None
    }

    fn apply_delta(&mut self, d: &Delta) -> DeltaOutcome {
        if self.seq().incarnation != Some(d.incarnation) {
            *self.seq() = SeqState {
                incarnation: Some(d.incarnation),
                last_seq: 0,
            };
        }
        if d.seq <= self.seq().last_seq {
            return DeltaOutcome::Stale;
        }
        let (Some(adds), Some(removes)) = (decode_keys(0, &d.adds), decode_keys(0, &d.removes))
        else {
            return DeltaOutcome::Malformed;
        };
        if adds.len() + removes.len() > MAX_DELTA_KEYS {
            return DeltaOutcome::Malformed;
        }
        let on_base = self.root_fp() == d.base;
        for k in removes {
            self.del(k, on_base);
        }
        let mut stored = true;
        for k in adds {
            stored &= self.add(k, on_base);
        }
        self.seq().last_seq = d.seq;
        if stored && on_base && self.root_fp() == d.root {
            DeltaOutcome::Exact
        } else {
            DeltaOutcome::Diverged
        }
    }

    fn matches(&mut self, s: &Summary) -> bool {
        let same = self.len() as u64 == s.count && self.root_fp() == s.root;
        if same {
            if self.seq().incarnation != Some(s.incarnation) {
                *self.seq() = SeqState {
                    incarnation: Some(s.incarnation),
                    last_seq: 0,
                };
            }
            let l = self.seq().last_seq.max(s.seq);
            self.seq().last_seq = l;
        }
        same
    }
}

/// Outcome of one repair, any method.
#[derive(Debug, Default, Clone)]
pub struct RepairStats {
    /// Request/reply round trips (RIBLT: batches).
    pub rounds: usize,
    pub req_bytes: usize,
    pub reply_bytes: usize,
    /// Keys carried in `Items` answers.
    pub items_keys: usize,
    /// Receiver-side CPU, nanoseconds.
    pub recv_ns: u64,
    /// Owner-side CPU, nanoseconds.
    pub owner_ns: u64,
    /// The session ended without settling (inconsistent peer, round cap
    /// or a shape the candidate cannot descend).
    pub failed: bool,
}

impl RepairStats {
    pub fn bytes(&self) -> usize {
        self.req_bytes + self.reply_bytes
    }
}

/// Bench copy of `reconcile::respond` plus one rule for mirrors that
/// cannot descend below `leaf_bits`: a differing range at that depth or
/// deeper is answered with its `Items` directly (the owner learns the
/// receiver's leaf depth when the session opens). With `leaf_bits =
/// None` it is byte for byte the real `respond`
/// (`tests::respond_leaf_equals_respond_without_leaf_depth`).
pub fn respond_leaf(
    set: &KeySet,
    summary: Summary,
    queries: &[Query],
    budget: usize,
    leaf_bits: Option<u8>,
) -> Reply {
    // Private constants of `reconcile.rs`, copied at the pinned commit.
    const CHILDREN_ANSWER_BYTES: usize = 1 + 11 + 1 + 16 * (16 + 10);
    const ITEMS_ANSWER_OVERHEAD: usize = 1 + 11 + 5;
    const KEY_BYTES_ESTIMATE: usize = 10;
    let mut answers = Vec::new();
    let mut used = 0usize;
    let mut processed = 0u32;
    for q in queries.iter().take(MAX_QUERIES_PER_REQUEST) {
        if !q.range.is_valid() {
            break;
        }
        let acc = set.acc(&q.range);
        if acc.count == q.count && acc.fingerprint() == q.fp {
            processed += 1;
            continue;
        }
        let fits = (acc.count as usize).saturating_mul(KEY_BYTES_ESTIMATE) + ITEMS_ANSWER_OVERHEAD
            <= budget;
        let items = acc.count <= constellation_net::reconcile::ITEMS_THRESHOLD
            || !q.range.can_split()
            || (q.count == 0 && fits)
            || (leaf_bits.is_some_and(|g| q.range.bits >= g) && fits);
        let (answer, size) = if items {
            let keys =
                constellation_net::reconcile::encode_keys(q.range.lo(), &set.keys_in(&q.range));
            let size = keys.len() + ITEMS_ANSWER_OVERHEAD;
            (
                Answer::Items {
                    range: q.range,
                    keys,
                },
                size,
            )
        } else {
            let children = q
                .range
                .children()
                .iter()
                .map(|c| {
                    let a = set.acc(c);
                    (a.fingerprint(), a.count)
                })
                .collect();
            (
                Answer::Children {
                    range: q.range,
                    children,
                },
                CHILDREN_ANSWER_BYTES,
            )
        };
        if !answers.is_empty() && used + size > budget {
            break;
        }
        used += size;
        answers.push(answer);
        processed += 1;
    }
    Reply {
        summary,
        processed,
        answers,
    }
}

/// Initiator side of RBSR for any `LocationMirror`, against the owner.
/// Depth first (plan 47 P3); the P1 tables were measured breadth first.
/// For a mirror with a `granularity` `g` the descent stops at depth
/// `g`: a range that would split below `g` is expanded into its
/// depth-`g` sub-ranges (queried with real fingerprints) and the owner,
/// told the leaf depth, answers a differing one with its `Items`.
/// `extra` ranges are fetched unconditionally (a bogus fingerprint and
/// count 0 force the owner to answer with items).
pub fn rbsr_repair<M: LocationMirror + ?Sized>(
    m: &mut M,
    owner: &KeySet,
    summary: Summary,
    start_bits: u8,
    extra: &[Range],
) -> RepairStats {
    let mut st = RepairStats::default();
    let gran = m.granularity();
    let bogus: Fingerprint = [0xFF; 16];
    // (range, forced)
    let mut queue: Vec<(Range, bool)> = Vec::new();
    let push = |queue: &mut Vec<(Range, bool)>, r: Range| match gran {
        Some(g) if r.bits + FANOUT_BITS > g && r.bits < g => {
            let d = u32::from(g - r.bits);
            for i in 0..(1u64 << d) {
                queue.push((
                    Range {
                        bits: g,
                        path: (r.path << d) | i,
                    },
                    false,
                ));
            }
        }
        _ => queue.push((r, false)),
    };
    for r in Range::level(start_bits.min(16)) {
        // A level deeper than the leaf depth cannot be queried.
        let r = match gran {
            Some(g) if r.bits > g => continue,
            _ => r,
        };
        push(&mut queue, r);
    }
    if gran.is_some_and(|g| start_bits > g) {
        push(&mut queue, Range::ROOT);
    }
    for r in extra {
        queue.push((*r, true));
    }
    while !queue.is_empty() {
        if st.rounds >= MAX_ROUNDS {
            st.failed = true;
            break;
        }
        let n = queue.len().min(MAX_QUERIES_PER_REQUEST);
        let t0 = Instant::now();
        // Depth first, as the product's `Session` since plan 47 P3: the
        // next request takes the most recently queued ranges.
        let queries: Vec<Query> = queue
            .iter()
            .rev()
            .take(n)
            .map(|&(r, forced)| {
                if forced {
                    Query {
                        range: r,
                        fp: bogus,
                        count: 0,
                    }
                } else {
                    let (fp, count) = m.acc(&r);
                    Query {
                        range: r,
                        fp,
                        count,
                    }
                }
            })
            .collect();
        st.recv_ns += t0.elapsed().as_nanos() as u64;
        st.req_bytes += wire_len(&Payload::ReconcileRequest {
            queries: queries.clone(),
        });
        let t1 = Instant::now();
        let reply: Reply = respond_leaf(owner, summary, &queries, REPLY_BUDGET, gran);
        st.owner_ns += t1.elapsed().as_nanos() as u64;
        st.reply_bytes += wire_len(&Payload::ReconcileReply {
            reply: reply.clone(),
        });
        st.rounds += 1;
        let processed = (reply.processed as usize).min(n);
        if processed == 0 {
            st.failed = true;
            break;
        }
        let t2 = Instant::now();
        let settled: Vec<(Range, bool)> = queue.split_off(queue.len() - processed);
        let mut by_range: HashMap<Range, &Answer> = HashMap::new();
        for a in &reply.answers {
            match a {
                Answer::Items { range, .. } | Answer::Children { range, .. } => {
                    by_range.insert(*range, a);
                }
            }
        }
        for (r, _) in settled {
            match by_range.get(&r) {
                None => {}
                Some(Answer::Items { range, keys }) => {
                    let Some(ks) = decode_keys(range.lo(), keys) else {
                        st.failed = true;
                        continue;
                    };
                    st.items_keys += ks.len();
                    m.replace_range(range, &ks);
                }
                Some(Answer::Children { range, children }) => {
                    let kids = range.children();
                    if kids.len() != children.len() {
                        st.failed = true;
                        continue;
                    }
                    for (child, (fp, count)) in kids.iter().zip(children) {
                        if *count == 0 {
                            m.replace_range(child, &[]);
                            continue;
                        }
                        let (mfp, mcount) = m.acc(child);
                        if mcount != *count || mfp != *fp {
                            push(&mut queue, *child);
                        }
                    }
                }
            }
        }
        st.recv_ns += t2.elapsed().as_nanos() as u64;
    }
    st
}

/// Sum of shard accumulators over a range of shards: helper for
/// shard-granular candidates.
pub fn sum_accs<'a>(it: impl Iterator<Item = &'a Acc>) -> (Fingerprint, u64) {
    let mut a = Acc::default();
    for x in it {
        a.merge(x);
    }
    (a.fingerprint(), a.count)
}

/// `rbsr_repair` starting where the daemon starts: `start_bits_for` the
/// mirror's own size.
pub fn rbsr_auto<M: LocationMirror + ?Sized>(
    m: &mut M,
    owner: &KeySet,
    summary: Summary,
    extra: &[Range],
) -> RepairStats {
    let sb = constellation_net::reconcile::start_bits_for(m.len());
    rbsr_repair(m, owner, summary, sb, extra)
}
