//! In-process multi-peer simulation with a virtual clock: one owner
//! publishes every 250 ms, each of `peers` receivers gets its messages
//! with independent loss and bounded reordering, and runs the same
//! session policy as the daemon (a session on a diverged delta or a
//! mismatching summary, at least 500 ms apart per peer). Loss and
//! reordering live here, in the harness; no product knob.

use crate::cand::Spec;
use crate::mirror::{rbsr_auto, LocationMirror};
use crate::publisher::{delta_wire, summary_wire, Churn, Publisher};
use crate::repair::replay::{replay_repair, DeltaLog};
use crate::util::{peer_keys, Rng};
use constellation_net::reconcile::{Delta, DeltaOutcome, KeySet, Range, Summary};
use std::collections::VecDeque;

pub const TICK_MS: u64 = 250;

#[derive(Clone)]
pub struct SimCfg {
    pub spec: Spec,
    pub n: usize,
    pub peers: usize,
    pub churn_pct: f64,
    pub loss: f64,
    /// Percent of messages displaced by 1..reorder-1 ticks (window `reorder`).
    pub reorder: usize,
    pub reorder_pct: f64,
    pub minutes: f64,
    pub interval_s: u64,
    pub rtt_ms: u64,
    pub seed: u64,
    pub replay: bool,
    pub lookups_per_tick: usize,
    pub add_frac: f64,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct Cnt {
    pub total: u64,
    pub wrong: u64,
}

#[derive(Default)]
pub struct SimOut {
    pub staleness_ms: Vec<f64>,
    pub unresolved: usize,
    pub events: usize,
    pub sessions: u64,
    pub session_rounds: u64,
    pub repair_bytes: u64,
    pub delta_bytes_per_peer: u64,
    pub summary_bytes_per_peer: u64,
    pub replay_ok: u64,
    pub replay_fail: u64,
    pub held: Cnt,
    pub removed: Cnt,
    pub absent: Cnt,
    pub final_fn: u64,
    pub final_mismatch_peers: usize,
    pub mirror_bytes: usize,
    pub grow_sessions: u64,
    pub minutes: f64,
    pub peers: usize,
}

enum Msg {
    Delta(Delta),
    Summary(Summary),
}

struct Peer {
    m: Box<dyn LocationMirror>,
    inbox: Vec<(u64, u64, Msg)>, // (due tick, order, msg)
    want_session: bool,
    extra: Vec<Range>,
    next_ok_ms: u64,
}

struct Ev {
    key: u64,
    want: bool,
    t0: u64,
    /// How long before the 250 ms drain the change happened.
    off: u64,
    done: Vec<bool>,
    left: usize,
}

pub fn run(cfg: &SimCfg) -> SimOut {
    let keys = peer_keys(cfg.seed, 0, cfg.n);
    let owner_set = KeySet::from_keys(keys.iter().copied());
    let mut publ = Publisher::new(owner_set, 1);
    let mut churn = Churn::new(cfg.seed ^ 0xC0FFEE, &publ.keys);
    churn.add_frac = cfg.add_frac;
    let mut rng = Rng::new(cfg.seed ^ 0x51ED);
    let mut peers: Vec<Peer> = (0..cfg.peers)
        .map(|_| Peer {
            m: {
                let mut m = cfg.spec.build(&keys, cfg.n);
                m.matches(&publ.summary());
                m
            },
            inbox: Vec::new(),
            want_session: false,
            extra: Vec::new(),
            next_ok_ms: 0,
        })
        .collect();
    let mut log = DeltaLog::new(16 << 20);
    let mut evs: VecDeque<Ev> = VecDeque::new();
    let mut recent_removed: VecDeque<(u64, u64)> = VecDeque::new();
    let mut out = SimOut {
        minutes: cfg.minutes,
        peers: cfg.peers,
        ..SimOut::default()
    };
    let ticks = (cfg.minutes * 240.0) as u64;
    let drain = 480u64; // two quiet minutes at the end
    let interval_ticks = cfg.interval_s * 4;
    let mut carry = 0.0;
    let mut order = 0u64;
    let horizon_ms = 5 * 60_000u64;
    for t in 0..(ticks + drain) {
        let now = t * TICK_MS;
        if t < ticks {
            let c = Churn::per_tick(cfg.n, cfg.churn_pct, &mut carry);
            let events = churn.tick(c);
            let absorbed = publ.absorb(&events, false, true);
            for &(k, want) in &events {
                evs.push_back(Ev {
                    key: k,
                    want,
                    t0: now,
                    off: rng.below(TICK_MS),
                    done: vec![false; cfg.peers],
                    left: cfg.peers,
                });
                if !want {
                    recent_removed.push_back((now, k));
                }
            }
            out.events += events.len() * cfg.peers;
            for d in absorbed.deltas {
                log.push(d.clone());
                out.delta_bytes_per_peer += delta_wire(&d) as u64;
                for p in &mut peers {
                    if rng.unit() >= cfg.loss {
                        order += 1;
                        let due = t + if cfg.reorder > 1 && rng.unit() * 100.0 < cfg.reorder_pct {
                            1 + rng.below(cfg.reorder as u64 - 1)
                        } else {
                            0
                        };
                        p.inbox.push((due, order, Msg::Delta(d.clone())));
                    }
                }
            }
        }
        while recent_removed.front().is_some_and(|f| now - f.0 > 60_000) {
            recent_removed.pop_front();
        }
        if t > 0 && t % interval_ticks == 0 {
            let s = publ.summary();
            out.summary_bytes_per_peer += summary_wire(&s) as u64;
            for p in &mut peers {
                if rng.unit() >= cfg.loss {
                    order += 1;
                    p.inbox.push((t, order, Msg::Summary(s)));
                    p.m.on_interval(&publ.keys);
                }
            }
        }
        // Deliver.
        for p in &mut peers {
            p.inbox.sort_by_key(|e| (e.0, e.1));
            let due = p.inbox.partition_point(|e| e.0 <= t);
            let msgs: Vec<(u64, u64, Msg)> = p.inbox.drain(..due).collect();
            for (_, _, m) in msgs {
                match m {
                    Msg::Delta(d) if cfg.replay => {
                        // Replay mode: a delta applies only in order on
                        // the exact base; a gap is not applied (the
                        // replay will carry it) and asks for a repair.
                        let v = p.m.seq_view();
                        if v.incarnation == Some(d.incarnation) && d.seq <= v.last_seq {
                            // stale or duplicate
                        } else if v.incarnation == Some(d.incarnation) && d.seq == v.last_seq + 1 {
                            if p.m.apply_delta(&d) == DeltaOutcome::Diverged {
                                p.want_session = true;
                            }
                        } else {
                            p.want_session = true;
                        }
                    }
                    Msg::Delta(d) => {
                        if p.m.apply_delta(&d) == DeltaOutcome::Diverged && p.m.repairs() {
                            p.want_session = true;
                        }
                    }
                    Msg::Summary(s) => {
                        let ls = p.m.seq_view().last_seq;
                        if p.m.repairs() && s.seq >= ls && !p.m.matches(&s) {
                            p.want_session = true;
                        }
                    }
                }
            }
            let dr = p.m.dirty_ranges();
            if !dr.is_empty() {
                out.grow_sessions += 1;
                p.extra.extend(dr);
                p.want_session = true;
            }
        }
        // Sessions.
        let mut session_extra = vec![0u64; peers.len()];
        for (pi, p) in peers.iter_mut().enumerate() {
            if !p.want_session || now < p.next_ok_ms || !p.m.repairs() {
                continue;
            }
            let summary = publ.summary();
            let mut done = false;
            let mut rounds = 0usize;
            if cfg.replay && p.extra.is_empty() {
                let (st, ok) = replay_repair(p.m.as_mut(), &log, summary);
                out.repair_bytes += st.bytes() as u64;
                rounds += st.rounds;
                if ok {
                    out.replay_ok += 1;
                    done = true;
                } else {
                    out.replay_fail += 1;
                }
            }
            if !done {
                let extra = std::mem::take(&mut p.extra);
                let st = rbsr_auto(p.m.as_mut(), &publ.keys, summary, &extra);
                out.repair_bytes += st.bytes() as u64;
                rounds += st.rounds;
                p.m.matches(&summary);
            }
            p.extra.clear();
            out.sessions += 1;
            out.session_rounds += rounds as u64;
            session_extra[pi] = rounds as u64 * cfg.rtt_ms;
            p.want_session = false;
            p.next_ok_ms = now + 500;
        }
        // Resolve events.
        for ev in evs.iter_mut() {
            for (pi, p) in peers.iter().enumerate() {
                if ev.done[pi] {
                    continue;
                }
                if p.m.contains(ev.key) == ev.want {
                    ev.done[pi] = true;
                    ev.left -= 1;
                    out.staleness_ms
                        .push((now - ev.t0 + ev.off + session_extra[pi]) as f64);
                }
            }
        }
        while evs
            .front()
            .is_some_and(|e| e.left == 0 || now - e.t0 > horizon_ms)
        {
            let e = evs.pop_front().unwrap();
            out.unresolved += e.left;
        }
        // Lookups (only while churn runs).
        if t < ticks {
            for p in &peers {
                for _ in 0..cfg.lookups_per_tick {
                    if !churn.cur.is_empty() {
                        let k = churn.cur[rng.below(churn.cur.len() as u64) as usize];
                        out.held.total += 1;
                        out.held.wrong += u64::from(!p.m.contains(k));
                    }
                    if !recent_removed.is_empty() {
                        let k = recent_removed[rng.below(recent_removed.len() as u64) as usize].1;
                        out.removed.total += 1;
                        out.removed.wrong += u64::from(p.m.contains(k));
                    }
                    let k = rng.next();
                    out.absent.total += 1;
                    out.absent.wrong += u64::from(p.m.contains(k));
                }
            }
        }
    }
    out.unresolved += evs.iter().map(|e| e.left).sum::<usize>();
    // Final state: every key the owner holds must be found, and the
    // mirrors must agree with the owner.
    let probe = churn.cur.len().min(200_000);
    for p in &peers {
        let mut fn_ = 0u64;
        for i in 0..probe {
            if !p.m.contains(churn.cur[i * churn.cur.len() / probe]) {
                fn_ += 1;
            }
        }
        out.final_fn += fn_;
        if p.m.repairs() && p.m.root_fp() != publ.keys.root_fingerprint() {
            out.final_mismatch_peers += 1;
        }
    }
    out.mirror_bytes = peers.first().map_or(0, |p| p.m.self_bytes());
    out
}
