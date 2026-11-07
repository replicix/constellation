//! `coop-digest`: Plan 47 P1 benchmark driver. One subcommand per
//! metric; every number in RESULTS.md comes from one of these.
//!
//!   mem          receiver memory per peer (allocator and RSS)
//!   lookup       lookup latency (fan-out over P peers), FP/FN vs oracle
//!   steady       publisher/receiver CPU per tick and wire bytes
//!   repair       repair bytes / round trips / CPU per method
//!   sim          staleness under loss and reordering, many peers
//!   cuckoo-load  filter insert failures and rebuilds at 50-95% load

use constellation_net::message::wire_len;
use constellation_net::reconcile::{
    respond_replay, DeltaLog as ProductLog, DeltaOutcome, KeySet, Mirror, Range, Summary,
    MAX_REPLAY_PAGES, REPLAY_PAGE_BUDGET,
};
use constellation_net::Payload;
use coop_digest::cand::bloomc::{self, BloomMirror};
use coop_digest::cand::cuckoo::LoneFilter;
use coop_digest::cand::fuse::{build_snapshot, snapshot_wire, FuseSet};
use coop_digest::cand::Spec;
use coop_digest::mirror::{rbsr_auto, LocationMirror};
use coop_digest::publisher::{delta_wire, summary_wire, Churn, Publisher};
use coop_digest::repair::replay::{replay_repair, DeltaLog};
use coop_digest::repair::riblt::riblt_repair;
use coop_digest::sim::{self, SimCfg};
use coop_digest::util::{
    human, live_bytes, peer_keys, quantile, rss_bytes, spread, Args, CountingAlloc, Rng,
};
use std::hint::black_box;
use std::time::Instant;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// `MAX_MIRROR_KEYS` in `crates/engine/src/coop/exact.rs`.
const MAX_MIRROR_KEYS: usize = 8 * 1024 * 1024;

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cmd = argv.first().cloned().unwrap_or_default();
    let a = Args(argv.iter().skip(1).cloned().collect());
    match cmd.as_str() {
        "mem" => cmd_mem(&a),
        "lookup" => cmd_lookup(&a),
        "steady" => cmd_steady(&a),
        "repair" => cmd_repair(&a),
        "sim" => cmd_sim(&a),
        "cuckoo-load" => cmd_cuckoo_load(&a),
        _ => {
            eprintln!(
                "usage: coop-digest <mem|lookup|steady|repair|sim|cuckoo-load> [--key value]..."
            );
            std::process::exit(2);
        }
    }
}

fn spec_arg(a: &Args) -> Spec {
    let s = a.str("spec", "c1");
    Spec::parse(&s).unwrap_or_else(|| panic!("unknown spec {s}"))
}

fn cmd_mem(a: &Args) {
    let spec = spec_arg(a);
    let n: usize = a.num("n", 1_000_000);
    let peers: usize = a.num("peers", 1);
    let seed: u64 = a.num("seed", 1);
    let mode = a.str("mode", "bulk");
    if matches!(spec, Spec::Exact) && n > MAX_MIRROR_KEYS && !a.flag("force") {
        println!(
            "| {:?} | {n} | {peers} | not mirrored: n exceeds MAX_MIRROR_KEYS ({MAX_MIRROR_KEYS}), peer treated as unknown | | | |",
            spec
        );
        return;
    }
    let live0 = live_bytes();
    let rss0 = rss_bytes();
    let mut mirrors: Vec<Box<dyn LocationMirror>> = Vec::new();
    let mut sync_note = String::new();
    for p in 0..peers {
        let keys = peer_keys(seed, p as u64, n);
        if mode == "bulk" {
            mirrors.push(spec.build(&keys, n));
        } else {
            let owner = KeySet::from_keys(keys.iter().copied());
            drop(keys);
            let s = Summary {
                incarnation: 1,
                seq: 1,
                root: owner.root_fingerprint(),
                count: owner.len() as u64,
            };
            let mut m = spec.empty(n);
            let (mut sessions, mut rounds, mut bytes) = (0, 0, 0);
            while !m.matches(&s) && sessions < 200 {
                let st = rbsr_auto(m.as_mut(), &owner, s, &[]);
                sessions += 1;
                rounds += st.rounds;
                bytes += st.bytes();
            }
            if p == 0 {
                sync_note = format!(
                    "initial sync: {sessions} sessions, {rounds} rounds, {}",
                    human(bytes as f64)
                );
            }
            mirrors.push(m);
        }
    }
    let live = live_bytes() - live0;
    let rss = rss_bytes().saturating_sub(rss0);
    let selfb: usize = mirrors.iter().map(|m| m.self_bytes()).sum();
    let total = (peers * n) as f64;
    println!(
        "| {} | {n} | {peers} | {mode} | {:.2} | {:.2} | {:.2} | {} | {} |{}",
        mirrors[0].label(),
        live as f64 / total,
        rss as f64 / total,
        selfb as f64 / total,
        human(live as f64),
        human(rss as f64),
        if sync_note.is_empty() {
            String::new()
        } else {
            format!(" {sync_note}")
        }
    );
}

fn fanout(ms: &[Box<dyn LocationMirror>], k: u64) -> usize {
    ms.iter().filter(|m| m.contains(k)).count()
}

fn cmd_lookup(a: &Args) {
    let spec = spec_arg(a);
    let n: usize = a.num("n", 1_000_000);
    let peers: usize = a.num("peers", 8);
    let ops: usize = a.num("ops", 200_000);
    let fp_ops: usize = a.num("fp-ops", 0);
    let seed: u64 = a.num("seed", 1);
    let mut mirrors: Vec<Box<dyn LocationMirror>> = Vec::new();
    let mut hits: Vec<(usize, u64)> = Vec::new();
    let mut r = Rng::new(seed ^ 0xABCD);
    let per_peer_samples = (ops / peers).max(1) + 1;
    for p in 0..peers {
        let keys = peer_keys(seed, p as u64, n);
        for _ in 0..per_peer_samples {
            hits.push((p, keys[r.below(keys.len() as u64) as usize]));
        }
        mirrors.push(spec.build(&keys, n));
    }
    // Shuffle the hit sample so peers interleave.
    for i in (1..hits.len()).rev() {
        let j = r.below(i as u64 + 1) as usize;
        hits.swap(i, j);
    }
    hits.truncate(ops);
    let misses: Vec<u64> = (0..ops).map(|_| r.next()).collect();
    // Warm up.
    for &(_, k) in hits.iter().take(20_000) {
        black_box(fanout(&mirrors, k));
    }
    let mut hit_ns = Vec::with_capacity(ops);
    for &(_, k) in &hits {
        let t = Instant::now();
        black_box(fanout(&mirrors, black_box(k)));
        hit_ns.push(t.elapsed().as_nanos() as f64);
    }
    let mut miss_ns = Vec::with_capacity(ops);
    for &k in &misses {
        let t = Instant::now();
        black_box(fanout(&mirrors, black_box(k)));
        miss_ns.push(t.elapsed().as_nanos() as f64);
    }
    // Single-peer batch mean (no per-call timer overhead).
    let t = Instant::now();
    let mut c = 0usize;
    for &k in &misses {
        c += usize::from(mirrors[0].contains(black_box(k)));
    }
    black_box(c);
    let single_miss = t.elapsed().as_nanos() as f64 / misses.len() as f64;
    let t = Instant::now();
    let mut c = 0usize;
    for &(p, k) in hits.iter().filter(|h| h.0 == 0) {
        c += usize::from(mirrors[p].contains(black_box(k)));
    }
    black_box(c);
    let h0 = hits.iter().filter(|h| h.0 == 0).count().max(1);
    let single_hit = t.elapsed().as_nanos() as f64 / h0 as f64;
    // Accuracy against the oracle (hits: the key's owner must contain
    // it; every other peer's claim is a false positive, the sets being
    // disjoint; misses: random keys absent from every set).
    let mut fnc = 0u64;
    let mut fp_cross = 0u64;
    for &(p, k) in &hits {
        if !mirrors[p].contains(k) {
            fnc += 1;
        }
        fp_cross += (fanout(&mirrors, k) - usize::from(mirrors[p].contains(k))) as u64;
    }
    let mut fp_miss = 0u64;
    let mut miss_lookups = 0u64;
    for &k in misses
        .iter()
        .chain((0..fp_ops).map(|_| r.next()).collect::<Vec<_>>().iter())
    {
        fp_miss += fanout(&mirrors, k) as u64;
        miss_lookups += peers as u64;
    }
    let cross_lookups = (hits.len() * (peers - 1)) as u64;
    let fp_total = fp_miss + fp_cross;
    let lookups = miss_lookups + cross_lookups;
    println!(
        "| {} | {n} | {peers} | {:.0} / {:.0} | {:.0} / {:.0} | {:.1} / {:.1} | {fnc} | {fp_total} / {lookups} | {:.2e} |",
        mirrors[0].label(),
        quantile(&hit_ns, 0.5),
        quantile(&hit_ns, 0.99),
        quantile(&miss_ns, 0.5),
        quantile(&miss_ns, 0.99),
        single_hit,
        single_miss,
        fp_total as f64 / lookups as f64,
    );
}

fn percentile_row(v: &[f64]) -> String {
    format!(
        "{:.0} / {:.0} / {:.0}",
        quantile(v, 0.5),
        quantile(v, 0.99),
        v.iter().sum::<f64>() / v.len().max(1) as f64
    )
}

enum Mir {
    Dyn(Box<dyn LocationMirror>),
    Fuse(FuseSet),
    Bloom(BloomMirror),
}

impl Mir {
    fn m(&mut self) -> &mut dyn LocationMirror {
        match self {
            Mir::Dyn(b) => b.as_mut(),
            Mir::Fuse(f) => f,
            Mir::Bloom(b) => b,
        }
    }
}

fn cmd_steady(a: &Args) {
    let spec = spec_arg(a);
    let n: usize = a.num("n", 1_000_000);
    let pct: f64 = a.num("churn", 1.0);
    let minutes: f64 = a.num("minutes", 2.0);
    let interval_s: u64 = a.num("interval", 30);
    let resnap: u32 = a.num("resnap", 64);
    let seed: u64 = a.num("seed", 1);
    let keys = peer_keys(seed, 0, n);
    let live_keys = live_bytes();
    let live0 = live_keys;
    let mut publ = Publisher::new(KeySet::from_keys(keys.iter().copied()), 1);
    let pub_bytes = live_bytes() - live0;
    let live1 = live_bytes();
    let mut mir = match &spec {
        Spec::Fuse(t) => Mir::Fuse(FuseSet::from_sorted(&keys, n, *t)),
        Spec::Bloom => Mir::Bloom(BloomMirror::from_keys(&keys)),
        s => Mir::Dyn(s.build(&keys, n)),
    };
    let mirror_bytes0 = live_bytes() - live1;
    let mut churn = Churn::new(seed ^ 7, &publ.keys);
    churn.add_frac = a.num("add-frac", 0.5);
    let mut log = DeltaLog::new(1 << 30);
    // Plan 47 P3: the product's bounded delta log, charged to the
    // publisher's time (A/B with `--plog 0`).
    let use_plog = a.num::<u32>("plog", 1) != 0;
    let plog_cap: usize = a.num(
        "plog-bytes",
        constellation_net::reconcile::MAX_REPLAY_BACKLOG,
    );
    let mut plog = ProductLog::new(plog_cap, std::time::Duration::from_secs(600));
    let mut plog_peak = 0usize;
    let clock0 = Instant::now();
    let ticks = (minutes * 240.0) as u64;
    let interval_ticks = interval_s * 4;
    let (mut pub_ns, mut recv_ns) = (Vec::new(), Vec::new());
    let (mut delta_w, mut summ_w, mut snap_w, mut changes_total, mut non_exact) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut snaps, mut snap_ns) = (0u64, 0u64);
    let mut bucket_changes: Vec<u32> = Vec::new();
    let mut carry = 0.0;
    let mut snap_round_ns = Vec::new();
    for t in 1..=ticks {
        let c = Churn::per_tick(n, pct, &mut carry);
        let ev = churn.tick(c);
        changes_total += ev.len() as u64;
        let t0 = Instant::now();
        let out = publ.absorb(&ev, false, true);
        if use_plog {
            // Simulated time, 250 ms per tick, so the age bound is exercised.
            let now = clock0 + std::time::Duration::from_millis(250 * t);
            plog.prune(now);
            if out.silent {
                plog.clear();
            }
            for d in &out.deltas {
                plog.push(d.clone(), now);
            }
            plog_peak = plog_peak.max(plog.bytes());
        }
        pub_ns.push(t0.elapsed().as_nanos() as f64);
        let t1 = Instant::now();
        for d in &out.deltas {
            match &mut mir {
                Mir::Bloom(b) => {
                    let adds =
                        constellation_net::reconcile::decode_keys(0, &d.adds).unwrap_or_default();
                    delta_w += bloomc::delta_wire(&adds) as u64;
                    b.apply_delta(d);
                }
                other => {
                    delta_w += delta_wire(d) as u64;
                    if other.m().apply_delta(d) != DeltaOutcome::Exact {
                        non_exact += 1;
                    }
                }
            }
            log.push(d.clone());
        }
        recv_ns.push(t1.elapsed().as_nanos() as f64);
        if let Mir::Fuse(f) = &mut mir {
            // Owner re-snapshots a bucket after `resnap` changes to it.
            let bb = f.bucket_bits();
            if bucket_changes.is_empty() {
                bucket_changes = vec![0; 1usize << bb];
            }
            let mut due = Vec::new();
            for &(k, _) in &ev {
                let i = coop_digest::cand::shard_idx(k, bb);
                bucket_changes[i] += 1;
                if bucket_changes[i] == resnap {
                    due.push(i);
                }
            }
            for i in due {
                let r = Range {
                    bits: bb,
                    path: i as u64,
                };
                let t = Instant::now();
                let ks = publ.keys.keys_in(&r);
                let snap = build_snapshot(&ks);
                snap_ns += t.elapsed().as_nanos() as u64;
                snaps += 1;
                if let Some(s) = &snap {
                    snap_w += snapshot_wire(s) as u64;
                }
                f.apply_snapshot(i, snap);
                bucket_changes[i] = 0;
            }
        }
        if t % interval_ticks == 0 {
            summ_w += summary_wire(&publ.summary()) as u64;
            if let Mir::Bloom(b) = &mut mir {
                let t = Instant::now();
                b.on_interval(&publ.keys);
                snap_round_ns.push(t.elapsed().as_nanos() as f64);
                snap_w += b.snapshot_round_wire() as u64;
            }
        }
    }
    let overlay = match &mir {
        Mir::Fuse(f) => f.overlay_entries(),
        _ => 0,
    };
    // Publisher, churn state and delta log gone: what remains above the
    // key vector is the mirror alone.
    let log_bytes = log.bytes();
    let (plog_now, plog_len) = (plog.bytes(), plog.len());
    let live_with_log = live_bytes();
    drop(plog);
    let plog_live = live_with_log.saturating_sub(live_bytes());
    drop(churn);
    drop(log);
    drop(publ);
    let mirror_bytes1 = live_bytes().saturating_sub(live_keys);
    let _ = live1;
    let label = mir.m().label();
    let per_min = |x: u64| x as f64 / minutes;
    println!(
        "| {label} | {n} | {pct}% | {:.1} | {} | {} | {:.0} | {:.0} | {:.0} | {:.2} / {:.2} | {} |{}{}",
        changes_total as f64 / ticks as f64,
        percentile_row(&pub_ns),
        percentile_row(&recv_ns),
        per_min(delta_w),
        per_min(summ_w),
        per_min(snap_w),
        mirror_bytes0 as f64 / n as f64,
        mirror_bytes1 as f64 / n as f64,
        human(pub_bytes as f64),
        if non_exact > 0 { format!(" NON-EXACT deltas: {non_exact}") } else { String::new() },
        match &mir {
            Mir::Fuse(_) => format!(
                " resnap={resnap}: {snaps} snapshots, build {:.2} ms each, overlay {overlay} entries at end",
                snap_ns as f64 / snaps.max(1) as f64 / 1e6
            ),
            Mir::Bloom(_) => format!(" bloom snapshot rebuild {:.1} ms per interval", quantile(&snap_round_ns, 0.5) / 1e6),
            _ => format!(
                " delta log {:.0} B/min; product log {} B counted, {} B allocator-live, peak {} B ({} entries) cap {} B",
                per_min(log_bytes as u64),
                plog_now,
                plog_live,
                plog_peak,
                plog_len,
                if use_plog { plog_cap } else { 0 }
            ),
        }
    );
}

struct World {
    base: Vec<u64>,
    base_summary: Summary,
    publ: Publisher,
    log: DeltaLog,
    /// The product's `reconcile::DeltaLog`, filled as `LocalSet::absorb`
    /// does (a silent tick clears it).
    plog: ProductLog,
    silent: bool,
}

/// What the daemon does for one gap (plan 47 P3): replay the owner's delta
/// log if the mirror's incarnation is the owner's, and run range sessions
/// (the real `Session` through `sync_in_memory`, as the daemon repeats
/// them) when replay cannot finish. Bytes are signed-frame sizes for the
/// replay messages and postcard sizes for the session rounds.
struct ProductRun {
    via: &'static str,
    sessions: usize,
    rounds: usize,
    replay_bytes: usize,
    session_bytes: usize,
    ok: bool,
    cpu_ms: f64,
    fneg: usize,
}

fn product_repair(w: &World, n: usize) -> ProductRun {
    let owner = &w.publ.keys;
    let summary = w.publ.summary();
    let mut mirror = Mirror {
        keys: KeySet::from_keys(w.base.iter().copied()),
        incarnation: Some(w.base_summary.incarnation),
        last_seq: w.base_summary.seq,
        exact_seq: Some(w.base_summary.seq),
    };
    let t0 = Instant::now();
    let mut run = ProductRun {
        via: "session",
        sessions: 0,
        rounds: 0,
        replay_bytes: 0,
        session_bytes: 0,
        ok: false,
        cpu_ms: 0.0,
        fneg: 0,
    };
    let mut replayed = false;
    // The daemon skips replay when the owner's incarnation changed.
    if let Some((inc, mut after)) = mirror.replay_point() {
        if inc == summary.incarnation {
            replayed = true;
            for _ in 0..MAX_REPLAY_PAGES {
                let req = Payload::ReplayRequest {
                    incarnation: inc,
                    after_seq: after,
                };
                let reply = respond_replay(&w.plog, summary, inc, after, REPLAY_PAGE_BUDGET);
                run.rounds += 1;
                run.replay_bytes += wire_len(&req);
                let reply_msg = Payload::ReplayReply { reply };
                run.replay_bytes += wire_len(&reply_msg);
                let Payload::ReplayReply { reply } = reply_msg else {
                    unreachable!()
                };
                if !reply.available {
                    break;
                }
                let Ok(last) = mirror.apply_replay(&reply.deltas, after) else {
                    break;
                };
                let progressed = last > after;
                after = last;
                if after >= reply.summary.seq {
                    if mirror.matches(&reply.summary) {
                        run.via = "replay";
                    }
                    break;
                }
                if !progressed {
                    break;
                }
            }
        }
    }
    if run.via != "replay" {
        run.via = if replayed {
            "replay, then session"
        } else {
            "session"
        };
        while mirror.keys.root_fingerprint() != owner.root_fingerprint() && run.sessions < 50 {
            let sb = constellation_net::reconcile::start_bits_for(mirror.keys.len());
            let (r, b) = constellation_net::reconcile::sync_in_memory(&mut mirror.keys, owner, sb);
            run.sessions += 1;
            run.rounds += r;
            run.session_bytes += b;
        }
    }
    run.cpu_ms = t0.elapsed().as_secs_f64() * 1e3;
    run.ok = mirror.keys.root_fingerprint() == owner.root_fingerprint();
    run.fneg = owner
        .iter()
        .step_by((n / 20_000).max(1))
        .filter(|&k| !mirror.keys.contains(k))
        .count();
    run
}

fn make_world(base: &[u64], scenario: &str, seed: u64) -> World {
    let n = base.len();
    let mut publ = Publisher::new(KeySet::from_keys(base.iter().copied()), 1);
    let base_summary = publ.summary();
    let mut churn = Churn::new(seed ^ 0x77, &publ.keys);
    let mut log = DeltaLog::new(1 << 30);
    let mut plog = ProductLog::new(1 << 30, std::time::Duration::from_secs(3600));
    let mut silent = false;
    let mut tick = |publ: &mut Publisher, log: &mut DeltaLog, d: usize, silent: &mut bool| {
        let ev = churn.tick(d);
        let out = publ.absorb(&ev, false, true);
        *silent |= out.silent;
        if out.silent {
            plog.clear();
        }
        for x in out.deltas {
            plog.push(x.clone(), Instant::now());
            log.push(x);
        }
    };
    match scenario {
        "gap1" => tick(&mut publ, &mut log, 1, &mut silent),
        "gap1k" => tick(&mut publ, &mut log, 1000, &mut silent),
        "gap100k" => {
            for _ in 0..7 {
                tick(&mut publ, &mut log, 100_000 / 7, &mut silent);
            }
        }
        "burst100k" => tick(&mut publ, &mut log, 100_000, &mut silent),
        s if s.starts_with("gap") && s.ends_with('k') => {
            // gap<N>k: N thousand changes spread over ticks of at most 14k.
            let d: usize = s[3..s.len() - 1].parse::<usize>().expect("gap<N>k") * 1000;
            let ticks = d.div_ceil(14_000);
            for _ in 0..ticks {
                tick(&mut publ, &mut log, d / ticks, &mut silent);
            }
        }
        "restart" => {
            tick(&mut publ, &mut log, n / 50, &mut silent);
            publ.incarnation = 2;
            publ.seq = 1;
            silent = true;
        }
        "initial" => {}
        s => panic!("unknown scenario {s}"),
    }
    World {
        base: base.to_vec(),
        base_summary,
        publ,
        log,
        plog,
        silent,
    }
}

fn cmd_repair(a: &Args) {
    let n: usize = a.num("n", 1_000_000);
    let seed: u64 = a.num("seed", 1);
    let scenarios = a.str("scenarios", "gap1,gap1k,gap100k,burst100k,restart,initial");
    let specs = a.str("specs", "c1,c6a,c6b,c3,c4,c5,c8a");
    let methods = a.str("methods", "rbsr,real,riblt,replay,product");
    let base = peer_keys(seed, 0, n);
    for sc in scenarios.split(',') {
        let w = make_world(&base, sc, seed);
        let summary = w.publ.summary();
        if methods.split(',').any(|m| m == "product") && sc != "initial" {
            // The daemon's repair path with the product code: replay first,
            // range sessions when replay cannot finish (exact mirror only).
            let r = product_repair(&w, n);
            println!(
                "| {sc} | exact | product | {} | {} | {} | {} | {:.1} | {} B replay + {} B session | {} |",
                if r.ok && r.fneg == 0 { "ok" } else { "FAILED" },
                r.sessions,
                r.rounds,
                human((r.replay_bytes + r.session_bytes) as f64),
                r.cpu_ms,
                r.replay_bytes,
                r.session_bytes,
                r.via,
            );
        }
        let diff = if sc == "initial" { n } else { 0 };
        let _ = diff;
        for sp in specs.split(',') {
            let spec = Spec::parse(sp).unwrap_or_else(|| panic!("spec {sp}"));
            for me in methods.split(',') {
                if me == "product" {
                    continue;
                }
                let mut m = if sc == "initial" {
                    spec.empty(n)
                } else {
                    spec.build(&w.base, n)
                };
                if sc != "initial" {
                    m.matches(&w.base_summary);
                }
                let label = m.label();
                let res: Option<(coop_digest::mirror::RepairStats, bool, usize, Option<usize>)> =
                    match me {
                        "rbsr" => {
                            let (mut sessions, mut tot) =
                                (0usize, coop_digest::mirror::RepairStats::default());
                            while !m.matches(&summary) && sessions < 200 {
                                let st = rbsr_auto(m.as_mut(), &w.publ.keys, summary, &[]);
                                sessions += 1;
                                tot.rounds += st.rounds;
                                tot.req_bytes += st.req_bytes;
                                tot.reply_bytes += st.reply_bytes;
                                tot.items_keys += st.items_keys;
                                tot.recv_ns += st.recv_ns;
                                tot.owner_ns += st.owner_ns;
                            }
                            let ok = m.matches(&summary);
                            Some((tot, ok, sessions, None))
                        }
                        "real" => {
                            // The real `Session` driver (`sync_in_memory`), repeated
                            // as the daemon would, on the real mirror type. Bytes
                            // are postcard lengths without the signed envelope.
                            if !matches!(spec, Spec::Exact) || sc == "initial" {
                                None
                            } else {
                                let mut ks = KeySet::from_keys(w.base.iter().copied());
                                let (mut sessions, mut tot) =
                                    (0usize, coop_digest::mirror::RepairStats::default());
                                let t0 = Instant::now();
                                while ks.root_fingerprint() != w.publ.keys.root_fingerprint()
                                    && sessions < 50
                                {
                                    let sb = constellation_net::reconcile::start_bits_for(ks.len());
                                    let (r, b) = constellation_net::reconcile::sync_in_memory(
                                        &mut ks,
                                        &w.publ.keys,
                                        sb,
                                    );
                                    sessions += 1;
                                    tot.rounds += r;
                                    tot.reply_bytes += b;
                                }
                                tot.recv_ns = t0.elapsed().as_nanos() as u64;
                                let ok = ks.root_fingerprint() == w.publ.keys.root_fingerprint();
                                let fneg = w
                                    .publ
                                    .keys
                                    .iter()
                                    .step_by((n / 20_000).max(1))
                                    .filter(|&k| !ks.contains(k))
                                    .count();
                                Some((tot, ok, sessions, Some(fneg)))
                            }
                        }
                        "riblt" => {
                            if m.keys().is_none() || (sc == "initial" && n > 2_000_000) {
                                None
                            } else {
                                let (st, ok) = riblt_repair(m.as_mut(), &w.publ.keys, 32, 3072);
                                Some((st, ok, 1, None))
                            }
                        }
                        "replay" => {
                            if matches!(sc, "initial" | "restart") || w.silent || !m.repairs() {
                                None
                            } else {
                                let (st, ok) = replay_repair(m.as_mut(), &w.log, summary);
                                Some((st, ok, 1, None))
                            }
                        }
                        other => panic!("method {other}"),
                    };
                match res {
                    None => println!("| {sc} | {label} | {me} | n/a | | | | | | |"),
                    Some((st, ok, sessions, fn_override)) => {
                        // Verify: no false negative on a sample of the owner's keys.
                        let mut fneg = 0;
                        for k in w.publ.keys.iter().step_by((n / 20_000).max(1)) {
                            if !m.contains(k) {
                                fneg += 1;
                            }
                        }
                        if let Some(f) = fn_override {
                            fneg = f;
                        }
                        let cpu_ms = (st.owner_ns + st.recv_ns) as f64 / 1e6;
                        println!(
                            "| {sc} | {label} | {me} | {} | {} | {} | {} | {:.1} / {:.1} | {:.1} / {:.1} | {} |",
                            if ok && fneg == 0 { "ok" } else { "FAILED" },
                            sessions,
                            st.rounds,
                            human(st.bytes() as f64),
                            st.owner_ns as f64 / 1e6,
                            st.recv_ns as f64 / 1e6,
                            cpu_ms + st.rounds as f64 * 1.0,
                            cpu_ms + st.rounds as f64 * 50.0,
                            fneg,
                        );
                    }
                }
            }
        }
    }
}

fn cmd_sim(a: &Args) {
    let spec = spec_arg(a);
    let cfg = SimCfg {
        spec: spec.clone(),
        n: a.num("n", 100_000),
        peers: a.num("peers", 8),
        churn_pct: a.num("churn", 1.0),
        loss: a.num("loss", 0.01),
        reorder: a.num("reorder", 4),
        reorder_pct: a.num("reorder-pct", 5.0),
        minutes: a.num("minutes", 10.0),
        interval_s: a.num("interval", 30),
        rtt_ms: a.num("rtt", 20),
        seed: a.num("seed", 1),
        replay: a.flag("replay"),
        lookups_per_tick: a.num("lookups", 20),
        add_frac: a.num("add-frac", 0.5),
    };
    let t = Instant::now();
    let o = sim::run(&cfg);
    let sm = &o.staleness_ms;
    let per_peer_min = |x: u64| x as f64 / cfg.minutes / cfg.peers as f64;
    let rate = |c: &sim::Cnt| format!("{}/{}", c.wrong, c.total);
    println!(
        "| {:?}{} | {} | {} | {}% | {}% / {}% | {:.0} / {:.0} / {:.0} | {} | {:.2} | {:.0} | {:.0} | {} | {} | {} | {} | {} | replay {}/{} ok, grow-sessions {}, {:.0}s wall",
        spec,
        if cfg.replay { "+replay" } else { "" },
        cfg.n,
        cfg.peers,
        cfg.churn_pct,
        cfg.loss * 100.0,
        if cfg.reorder > 1 { cfg.reorder_pct } else { 0.0 },
        quantile(sm, 0.5),
        quantile(sm, 0.99),
        sm.iter().cloned().fold(0.0, f64::max),
        o.unresolved,
        o.sessions as f64 / cfg.minutes / cfg.peers as f64,
        per_peer_min(o.repair_bytes),
        (o.delta_bytes_per_peer + o.summary_bytes_per_peer) as f64 / cfg.minutes,
        rate(&o.removed),
        rate(&o.held),
        rate(&o.absent),
        o.final_fn,
        o.final_mismatch_peers,
        o.replay_ok,
        o.replay_ok + o.replay_fail,
        o.grow_sessions,
        t.elapsed().as_secs_f64(),
    );
}

fn cmd_cuckoo_load(a: &Args) {
    let f: u32 = a.num("f", 16);
    let trials: usize = a.num("trials", 300);
    let ops: usize = a.num("ops", 2_000_000);
    println!("first failure (fill from empty; fraction of slots used at the first failed eviction chain), f={f}, {trials} trials");
    println!("| slots | min | p5 | median | trials failing before 90% | before 95% |");
    for slots in [256usize, 1024, 4096, 16384] {
        let mut fr = Vec::new();
        for t in 0..trials {
            let mut lf = LoneFilter::with_slots(slots, f);
            let mut r = Rng::new(t as u64 * 7919 + slots as u64);
            let cap = lf.capacity();
            while lf.insert(r.next()) {
                if lf.count as usize >= cap {
                    break;
                }
            }
            fr.push(lf.count as f64 / cap as f64);
        }
        let b90 = fr.iter().filter(|&&x| x < 0.90).count();
        let b95 = fr.iter().filter(|&&x| x < 0.95).count();
        println!(
            "| {slots} | {:.3} | {:.3} | {:.3} | {b90} | {b95} |",
            quantile(&fr, 0.0),
            quantile(&fr, 0.05),
            quantile(&fr, 0.5)
        );
    }
    println!();
    println!("steady churn at a fixed load: remove a random present key, insert a fresh one ({ops} cycles per cell; a failure leaves the shard to be rebuilt from the owner)");
    println!("| slots | target load | failures | failures per 1M cycles | rebuilds (refetches) |");
    for slots in [256usize, 4096] {
        for load in [0.50f64, 0.70, 0.80, 0.85, 0.90, 0.93, 0.95] {
            let target = (slots as f64 * load) as usize;
            let mut r = Rng::new(slots as u64 ^ (load * 1000.0) as u64);
            let mut present: Vec<u64> = Vec::new();
            let mut lf = LoneFilter::with_slots(slots, f);
            let (mut failures, mut rebuilds) = (0u64, 0u64);
            let build = |present: &mut Vec<u64>,
                         lf: &mut LoneFilter,
                         r: &mut Rng,
                         rebuilds: &mut u64,
                         fresh: bool| {
                if fresh {
                    *rebuilds += 1;
                }
                *lf = LoneFilter::with_slots(slots, f);
                // A rebuild re-inserts the surviving keys, then tops up.
                let keep: Vec<u64> = present.clone();
                present.clear();
                for k in keep {
                    if lf.insert(k) {
                        present.push(k);
                    }
                }
                while present.len() < target {
                    let k = r.next();
                    if lf.insert(k) {
                        present.push(k);
                    } else {
                        break;
                    }
                }
            };
            build(&mut present, &mut lf, &mut r, &mut rebuilds, false);
            rebuilds = 0;
            for _ in 0..ops {
                if present.is_empty() {
                    break;
                }
                let j = r.below(present.len() as u64) as usize;
                let k = present.swap_remove(j);
                lf.remove(k);
                let nk = r.next();
                if lf.insert(nk) {
                    present.push(nk);
                } else {
                    failures += 1;
                    present.push(nk);
                    // The shard is refetched and rebuilt at the target load.
                    build(&mut present, &mut lf, &mut r, &mut rebuilds, true);
                }
            }
            println!(
                "| {slots} | {:.0}% | {failures} | {:.1} | {rebuilds} |",
                load * 100.0,
                failures as f64 * 1e6 / ops as f64
            );
        }
    }
    let _ = spread(&[1.0]);
}
