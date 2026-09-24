//! Plan 30 §M7 scenarios: direct log streams and cross-node visibility.
//!
//! `visibility-after-burst` is the local form of the EC2 bench's row 6
//! (`bench/remote/RESULTS.md`): one node writes a large burst, then a
//! paced series of fsync'd marker files, and two other nodes poll for
//! each marker *while it is being written* — the latency is the time from
//! the marker's `open` on the writer to the first poll on another node
//! that lists it and reads its content. (Row 6 itself started its pollers
//! only after the writer had finished and the event list had been copied
//! to them, so its "latency" was mostly the writer's run time plus an 8 s
//! barrier: see PROGRESS.md, plan 30 M7.)
//!
//! It runs the measurement twice on fresh filesystems — log streams off
//! (`CONSTELLATION_LOG_STREAMS=0`: gossip hints plus the S3 GET-next
//! tailer) and on — and records each poller's S3 tail GETs (`GET` of a
//! `log/` key, through a `CountingProxy`) during the marker phase.

use super::{eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Files and size of the burst (write-back on the writer, so most of it
/// is still uploading when the markers start).
const BURST_FILES: usize = 24;
const BURST_FILE_MB: usize = 8;
/// Added S3 request latency, each direction.
const LATENCY_MS: u64 = 10;
/// The marker series: count and pacing.
const MARKERS: usize = 60;
const MARKER_INTERVAL: Duration = Duration::from_millis(100);
/// The visibility bound the scenario asserts (p99, every poller).
const P99_BOUND: Duration = Duration::from_secs(2);
/// A poller gives up on a marker after this long (reported as a timeout).
const POLL_DEADLINE: Duration = Duration::from_secs(60);

/// What one configuration measured.
struct Measured {
    label: &'static str,
    /// Per poller: sorted latencies of the markers it saw, and how many it
    /// never saw.
    pollers: Vec<(String, Vec<Duration>, usize)>,
    /// Per poller: S3 `GET`s of `log/` keys during the marker phase.
    tail_gets: Vec<(String, u64)>,
    /// Per poller: the log-stream counters from `status.log_stream`.
    stream: Vec<(String, serde_json::Value)>,
    burst: Duration,
}

/// A diagnosis override (`VIS_*` in the harness's environment) of one of
/// the constants above; the catalog run uses the constants.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

/// Deterministic incompressible bytes (the chunk store would deduplicate
/// or compress a pattern away).
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed | 1;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn tail_gets(counter: &CountingProxy) -> u64 {
    counter
        .requests()
        .iter()
        .filter(|r| r.method == "GET" && !r.is_list() && r.area() == "log")
        .count() as u64
}

fn run_once(seed: u64, streams: bool) -> Result<Measured> {
    let label = if streams { "streams on" } else { "streams off" };
    let name = if streams {
        "visibility-burst-on"
    } else {
        "visibility-burst-off"
    };
    let (env, root) = setup(name)?;
    let proxy = env.s3_proxy()?;
    // S3-like request latency on every node's path (in-region S3 is
    // 10-30 ms per request; floci on loopback is ~1 ms, which hides every
    // queueing effect a burst causes).
    proxy.latency(knob("VIS_LATENCY_MS", LATENCY_MS), 0)?;
    let backend = format!("s3://{BUCKET}/{name}-{}", ts());
    let counters = [
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    ];
    let mk = |who: &str, endpoint: &str| -> Result<Client> {
        let mut c = Client::new(root.path(), who, endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_LOG_STREAMS", if streams { "1" } else { "0" });
        // Diagnosis: `VIS_RUST_LOG=constellation_authority::stream=debug,
        // constellation::log_stream=debug` traces every segment from ship
        // through stream send and receive to apply (keep the logs with
        // `CHAOS_KEEP_TMP=1`).
        if let Ok(filter) = std::env::var("VIS_RUST_LOG") {
            c = c.with_env("RUST_LOG", &filter);
        }
        Ok(c)
    };
    let mut a = mk("a", &counters[0].endpoint())?.with_write_mode("back");
    let mut b = mk("b", &counters[1].endpoint())?;
    let mut c = mk("c", &counters[2].endpoint())?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    let result = (|| -> Result<Measured> {
        wait_for_p2p(&[&a, &b, &c])?;
        std::fs::create_dir(a.mnt.join("vis"))?;
        std::fs::create_dir(a.mnt.join("burst"))?;
        eventually("A holds the lease", Duration::from_secs(30), || {
            let lease = lease_of(&a)?;
            anyhow::ensure!(lease["held"] == true, "A does not hold: {lease}");
            Ok(())
        })?;
        for p in [&b, &c] {
            eventually(
                &format!("vis/ visible on {}", p.name),
                Duration::from_secs(30),
                || {
                    anyhow::ensure!(p.mnt.join("vis").is_dir(), "vis/ missing");
                    Ok(())
                },
            )?;
        }
        // The measurement is of a steady state: with streams on, both
        // pollers follow A's log stream first (a fresh mount subscribes
        // once A's registry has enrolled its key, within one registry
        // refresh); either way the followers' tailers settle.
        if streams {
            let a_id = a.control_status()?["node_id"].as_u64().unwrap_or(0);
            for p in [&b, &c] {
                eventually(
                    &format!("{} follows A's log stream", p.name),
                    Duration::from_secs(30),
                    || {
                        let s = p.control_status()?["log_stream"].clone();
                        anyhow::ensure!(
                            s["live"] == true && s["upstream"].as_u64() == Some(a_id),
                            "{}'s stream is not up: {s}",
                            p.name
                        );
                        Ok(())
                    },
                )?;
            }
        }
        std::thread::sleep(Duration::from_secs(2));

        // The burst.
        let started = Instant::now();
        let burst_files = knob("VIS_BURST_FILES", BURST_FILES as u64) as usize;
        for i in 0..burst_files {
            let data = noise(seed ^ ((i as u64 + 1) * 0x9e37_79b9), BURST_FILE_MB << 20);
            std::fs::write(a.mnt.join(format!("burst/f{i:03}")), &data)?;
        }
        let burst = started.elapsed();
        let pending = a.control_status()?["writeback"]["pending_uploads"].clone();
        eprintln!(
            "    visibility-after-burst ({label}): burst of {burst_files} x {BURST_FILE_MB} MiB \
             written in {burst:?}; A's pending uploads right after: {pending}"
        );

        // The marker series, with the pollers running meanwhile.
        for counter in &counters {
            counter.reset();
        }
        let written: Arc<Mutex<Vec<Option<Instant>>>> = Arc::new(Mutex::new(vec![None; MARKERS]));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut pollers = Vec::new();
        for p in [&b, &c] {
            let dir = p.mnt.join("vis");
            let who = p.name.clone();
            let done = done.clone();
            pollers.push(std::thread::spawn(move || {
                let mut seen: Vec<Option<Instant>> = vec![None; MARKERS];
                let deadline = Instant::now() + POLL_DEADLINE;
                while Instant::now() < deadline && seen.iter().any(|s| s.is_none()) {
                    if let Ok(entries) = std::fs::read_dir(&dir) {
                        for e in entries.flatten() {
                            let fname = e.file_name();
                            let Some(i) = fname
                                .to_str()
                                .and_then(|n| n.strip_prefix('m'))
                                .and_then(|n| n.parse::<usize>().ok())
                            else {
                                continue;
                            };
                            if i >= MARKERS || seen[i].is_some() {
                                continue;
                            }
                            if std::fs::read(e.path()).ok().as_deref()
                                == Some(format!("marker {i}").as_bytes())
                            {
                                seen[i] = Some(Instant::now());
                            }
                        }
                    }
                    if done.load(std::sync::atomic::Ordering::Relaxed)
                        && seen.iter().all(|s| s.is_some())
                    {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                (who, seen)
            }));
        }
        for i in 0..MARKERS {
            let t0 = Instant::now();
            written.lock().unwrap()[i] = Some(t0);
            let mut f = std::fs::File::create(a.mnt.join(format!("vis/m{i}")))?;
            f.write_all(format!("marker {i}").as_bytes())?;
            f.sync_all()?;
            drop(f);
            let spent = t0.elapsed();
            if std::env::var_os("VIS_DEBUG").is_some() {
                eprintln!("      marker {i}: written in {spent:?}");
            }
            if spent < MARKER_INTERVAL {
                std::thread::sleep(MARKER_INTERVAL - spent);
            }
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        let written = written.lock().unwrap().clone();
        let mut measured = Vec::new();
        for handle in pollers {
            let (who, seen) = handle
                .join()
                .map_err(|_| anyhow::anyhow!("poller panicked"))?;
            let mut lat = Vec::new();
            let mut missing = 0;
            for (w, s) in written.iter().zip(seen) {
                match (w, s) {
                    (Some(w), Some(s)) => lat.push(s.saturating_duration_since(*w)),
                    _ => missing += 1,
                }
            }
            if std::env::var_os("VIS_DEBUG").is_some() {
                eprintln!("      poller {who}: latencies in marker order {lat:?}");
            }
            lat.sort();
            measured.push((who, lat, missing));
        }
        let tail = vec![
            ("b".to_string(), tail_gets(&counters[1])),
            ("c".to_string(), tail_gets(&counters[2])),
        ];
        for (counter, who) in counters.iter().zip(["a", "b", "c"]) {
            eprintln!(
                "    visibility-after-burst ({label}): {who} S3 during the markers: {} | {}",
                crate::reqlog::tally(&counter.requests()),
                crate::reqlog::breakdown(&counter.requests())
            );
        }
        let stream = vec![
            ("b".to_string(), b.control_status()?["log_stream"].clone()),
            ("c".to_string(), c.control_status()?["log_stream"].clone()),
        ];
        Ok(Measured {
            label,
            pollers: measured,
            tail_gets: tail,
            stream,
            burst,
        })
    })();
    if result.is_err() {
        for cl in [&a, &b, &c] {
            eprintln!("--- {} log tail ---\n{}", cl.name, cl.tail_log_n(40));
        }
    }
    for cl in [&mut c, &mut b, &mut a] {
        cl.unmount().context("unmount")?;
    }
    result
}

/// Plan 30 §M7: `visibility-after-burst`.
pub fn visibility_after_burst(seed: u64) -> Result<()> {
    // `VIS_RUNS=off` measures only the stream-less configuration (what a
    // pre-M7 binary can do, for a before/after comparison).
    if std::env::var("VIS_RUNS").as_deref() == Ok("off") {
        let m = run_once(seed, false)?;
        for (who, lat, missing) in &m.pollers {
            eprintln!(
                "    visibility-after-burst ({}): poller {who}: {} seen, {missing} never; \
                 p50 {:?} p90 {:?} p99 {:?} max {:?}",
                m.label,
                lat.len(),
                pct(lat, 0.5),
                pct(lat, 0.9),
                pct(lat, 0.99),
                lat.last().copied().unwrap_or_default(),
            );
        }
        for (who, gets) in &m.tail_gets {
            eprintln!(
                "    visibility-after-burst ({}): poller {who}: S3 tail GETs {gets}",
                m.label
            );
        }
        return Ok(());
    }
    let runs = [run_once(seed, false)?, run_once(seed, true)?];
    let mut failures = Vec::new();
    for m in &runs {
        for (who, lat, missing) in &m.pollers {
            eprintln!(
                "    visibility-after-burst ({}): poller {who}: {} markers seen, {missing} never; \
                 p50 {:?} p90 {:?} p99 {:?} max {:?} (burst took {:?})",
                m.label,
                lat.len(),
                pct(lat, 0.5),
                pct(lat, 0.9),
                pct(lat, 0.99),
                lat.last().copied().unwrap_or_default(),
                m.burst,
            );
            if *missing > 0 {
                failures.push(format!("{}: {who} never saw {missing} marker(s)", m.label));
            }
            let p99 = pct(lat, 0.99);
            if p99 >= P99_BOUND {
                failures.push(format!(
                    "{}: {who}'s cross-node visibility p99 {p99:?} >= {P99_BOUND:?}",
                    m.label
                ));
            }
        }
        for ((who, gets), (_, stream)) in m.tail_gets.iter().zip(&m.stream) {
            eprintln!(
                "    visibility-after-burst ({}): poller {who}: S3 tail GETs during the markers: \
                 {gets}; stream counters {stream}",
                m.label
            );
        }
    }
    // With streams up the pollers read the log from the holder, not S3:
    // what is left is the width-1 backstop probe (at most one per
    // backstop period) and the probes of a stream coming up.
    let (off, on) = (&runs[0], &runs[1]);
    for ((who, gets_on), (_, gets_off)) in on.tail_gets.iter().zip(&off.tail_gets) {
        eprintln!(
            "    visibility-after-burst: poller {who}: S3 tail GETs during the markers, \
             streams off {gets_off} -> on {gets_on}"
        );
        if *gets_on > 4 {
            failures.push(format!(
                "streams on: {who} still issued {gets_on} S3 tail GETs during the markers"
            ));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}
