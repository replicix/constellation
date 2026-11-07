//! Delta-log replay: the owner keeps its last published deltas; a
//! receiver with a sequence gap asks for the deltas after its last
//! sequence and re-applies them on the exact base.

use crate::mirror::{LocationMirror, RepairStats};
use crate::publisher::delta_wire;
use constellation_net::message::SIGNED_ENVELOPE;
use constellation_net::reconcile::{Delta, Summary};
use serde::Serialize;
use std::collections::VecDeque;
use std::time::Instant;

#[derive(Default)]
pub struct DeltaLog {
    ring: VecDeque<Delta>,
    bytes: usize,
    cap_bytes: usize,
}

#[derive(Serialize)]
struct ReplayRequest {
    incarnation: u64,
    after_seq: u64,
}

impl DeltaLog {
    pub fn new(cap_bytes: usize) -> Self {
        Self {
            ring: VecDeque::new(),
            bytes: 0,
            cap_bytes,
        }
    }
    pub fn push(&mut self, d: Delta) {
        self.bytes += d.adds.len() + d.removes.len() + 80;
        self.ring.push_back(d);
        while self.bytes > self.cap_bytes && self.ring.len() > 1 {
            let o = self.ring.pop_front().unwrap();
            self.bytes -= o.adds.len() + o.removes.len() + 80;
        }
    }
    /// Retained bytes (payload plus the fixed fields of each delta).
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn len(&self) -> usize {
        self.ring.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
    /// Deltas with `seq > after`, if they form an unbroken chain from
    /// `after + 1` (a silent tick or a trimmed log breaks it).
    pub fn after(&self, incarnation: u64, after: u64) -> Option<Vec<Delta>> {
        let v: Vec<Delta> = self
            .ring
            .iter()
            .filter(|d| d.incarnation == incarnation && d.seq > after)
            .cloned()
            .collect();
        for (expect, d) in (after + 1..).zip(v.iter()) {
            if d.seq != expect {
                return None;
            }
        }
        Some(v)
    }
}

/// Try to repair `m` by replay. Returns the stats and whether the mirror
/// now matches `summary`; on `false` the caller falls back to RBSR.
pub fn replay_repair<M: LocationMirror + ?Sized>(
    m: &mut M,
    log: &DeltaLog,
    summary: Summary,
) -> (RepairStats, bool) {
    let mut st = RepairStats::default();
    let view = m.seq_view();
    let after = view.last_seq;
    let incarnation = view.incarnation.unwrap_or(0);
    let t = Instant::now();
    let deltas = if view.incarnation == Some(summary.incarnation) {
        log.after(summary.incarnation, after)
    } else {
        None
    };
    st.owner_ns += t.elapsed().as_nanos() as u64;
    st.req_bytes += postcard::to_allocvec(&ReplayRequest {
        incarnation,
        after_seq: after,
    })
    .map(|v| v.len())
    .unwrap_or(0)
        + SIGNED_ENVELOPE;
    st.rounds = 1;
    let Some(deltas) = deltas else {
        st.failed = true;
        return (st, false);
    };
    let total: usize = deltas.iter().map(delta_wire).sum();
    st.reply_bytes += total;
    st.rounds = st.rounds.max(total.div_ceil(48 * 1024));
    let t = Instant::now();
    for d in &deltas {
        m.apply_delta(d);
    }
    st.recv_ns += t.elapsed().as_nanos() as u64;
    let ok = m.matches(&summary);
    (st, ok)
}
