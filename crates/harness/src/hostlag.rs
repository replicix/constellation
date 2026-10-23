//! The host-lag probe and the `inconclusive` outcome (load campaign
//! phase 0).
//!
//! A scenario's wall-clock upper bound on a latency is a protocol minimum
//! (a TTL, a margin, a takeover window) plus slack for the host. On a
//! starved host (CPU throttling, hogs, a paused VM) the slack is not
//! enough and the bound fails although nothing is wrong but latency. The
//! requirement the campaign checks is that load costs latency only: so a
//! latency bound exceeded while the host was demonstrably starved is
//! reported **inconclusive (host lag X)**, never `FAILED`, and every
//! correctness assertion (data, exclusion, fencing, errno, hang) stays a
//! hard failure. A protocol *lower* bound ("not granted before TTL +
//! margin") is a contract and stays exact: lag only ever makes things
//! later.
//!
//! **The probe.** A thread sleeps [`PERIOD`] in a loop and records how
//! late each wake-up was (the overshoot): a starved host wakes it late.
//! [`begin`] starts it for a scenario and [`end`] stops it and summarises
//! (p50/p99/max overshoot, the time it lost in total, and the averages of
//! `/proc/pressure/{cpu,io,memory}` over the scenario). The load injectors
//! ([`crate::load`]) add sources of their own: a probe *process* inside
//! each throttled daemon's cgroup (`harness host-lag --stream`), which
//! sees the throttling the daemon sees, and the intervals they paused a
//! daemon for ([`note_injected`]).
//!
//! **A latency bound** ([`latency_bound`]) passes when the measurement is
//! within it. When it is not, the excess is compared with what the host
//! took from the measured window: the time the probes lost in it (the
//! worst source), plus any injected pause overlapping it ([`judge`] has
//! the rules). Lost time that covers the excess makes the bound
//! inconclusive in any lane; under a load profile, so do one stall longer
//! than the slack (the excess within one more protocol cycle) and an
//! applied injector the probe cannot see (runtime hog, store stall, S3 or
//! link faults: [`crate::load::injecting`]). Nothing excuses a measurement
//! past [`CAP_FACTOR`] × the bound (or the bound plus the lost time).
//! Otherwise it fails, naming the lag it saw. An inconclusive bound is
//! recorded ([`inconclusive`]) and the scenario goes on; a scenario whose
//! checks all passed with at least one is reported `inconclusive` in the
//! results file (`crate::results::Outcome::Inconclusive`), every note as
//! its reason (`harness run --fail-inconclusive` makes it a failure, and
//! `tests/parity.py --max-inconclusive` caps how many a lane may have).
//!
//! `harness host-lag [--seconds N]` runs the probe alone and prints the
//! summary as JSON (beside a soak, or to qualify a host);
//! `harness host-lag --stream` prints every sample (`<mono_ns> <late_us>`)
//! for a parent harness to read.

use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The probe's sleep.
pub const PERIOD: Duration = Duration::from_millis(50);

/// `CLOCK_MONOTONIC` in ns: the time base every source shares (a child
/// probe process's samples are on the same clock as the harness's
/// `Instant`s, which are `CLOCK_MONOTONIC` on Linux).
pub fn mono_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: a valid out-parameter.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// `t` on the [`mono_ns`] clock.
pub fn mono_of(t: Instant) -> u64 {
    let (now_i, now_m) = (Instant::now(), mono_ns());
    if t <= now_i {
        now_m.saturating_sub((now_i - t).as_nanos() as u64)
    } else {
        now_m + (t - now_i).as_nanos() as u64
    }
}

/// One wake-up: the sleep was due at `due` (mono ns) and ended `late_us`
/// later.
#[derive(Debug, Clone, Copy)]
struct Sample {
    due: u64,
    late_us: u32,
}

/// One probe's samples.
struct Source {
    name: String,
    samples: Vec<Sample>,
}

/// An interval a load injector held a daemon stopped (mono ns).
#[derive(Debug, Clone)]
struct Injected {
    node: String,
    what: String,
    from: u64,
    to: u64,
}

struct Probe {
    scenario: String,
    started: u64,
    sources: Mutex<Vec<Source>>,
    injected: Mutex<Vec<Injected>>,
    psi_start: Psi,
    stop: AtomicBool,
    /// The devices a disk probe already writes to.
    disks: Mutex<Vec<u64>>,
}

static CURRENT: Mutex<Option<Arc<Probe>>> = Mutex::new(None);
static NOTES: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn current() -> Option<Arc<Probe>> {
    CURRENT.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// A handle that adds samples to one source of the current probe.
#[derive(Clone)]
pub struct SourceHandle {
    probe: Arc<Probe>,
    index: usize,
}

impl SourceHandle {
    pub fn push(&self, due_mono_ns: u64, late: Duration) {
        let mut s = self.probe.sources.lock().unwrap_or_else(|e| e.into_inner());
        s[self.index].samples.push(Sample {
            due: due_mono_ns,
            late_us: late.as_micros().min(u32::MAX as u128) as u32,
        });
    }
}

/// A new source on the current scenario's probe (`None` outside one).
pub fn add_source(name: &str) -> Option<SourceHandle> {
    let probe = current()?;
    let mut s = probe.sources.lock().unwrap_or_else(|e| e.into_inner());
    s.push(Source {
        name: name.to_string(),
        samples: Vec::new(),
    });
    let index = s.len() - 1;
    drop(s);
    Some(SourceHandle { probe, index })
}

/// Sleep [`PERIOD`] until `stop`, handing each wake-up's lateness to
/// `sample` (mono ns it was due, how late).
pub fn probe_loop(stop: &AtomicBool, mut sample: impl FnMut(u64, Duration)) {
    while !stop.load(Ordering::Relaxed) {
        let due = mono_ns() + PERIOD.as_nanos() as u64;
        std::thread::sleep(PERIOD);
        let woke = mono_ns();
        sample(due, Duration::from_nanos(woke.saturating_sub(due)));
    }
}

/// The name of the disk-write probe's source.
const DISK_SOURCE: &str = "disk";

/// The disk-write probe: every [`PERIOD`] it overwrites 4 KiB of a file in
/// `dir` with a buffered `write(2)` (no sync) and records how long the call
/// took. A buffered write returns at once unless the filesystem is
/// throttling writers or is held up (dirty-page throttling behind a
/// stalled device, a journal commit, a btrfs transaction): the same
/// conditions that make a daemon's metadata writes and journal appends
/// slow without taking a CPU. A window in which this probe lost time is
/// one in which the disk, not the daemon, was slow, so a latency bound
/// exceeded there is inconclusive ([`latency_bound`]) **in a run with a
/// load profile only** (a load campaign: `CONSTELLATION_HARNESS_LOAD`); in
/// an ordinary run it is recorded in the summary and the failure message
/// but excuses nothing, as a buffered write also stalls on the daemon's own
/// sync traffic, and a daemon-caused I/O regression must still fail. One thread per
/// device of the current scenario's probe; none outside a scenario or with
/// `CONSTELLATION_HARNESS_NO_DISK_PROBE` set.
pub fn add_disk_probe(dir: &std::path::Path) {
    use std::os::unix::fs::{FileExt, MetadataExt};
    if std::env::var_os("CONSTELLATION_HARNESS_NO_DISK_PROBE").is_some() {
        return;
    }
    let Some(probe) = current() else {
        return;
    };
    let Ok(dev) = std::fs::metadata(dir).map(|m| m.dev()) else {
        return;
    };
    {
        let mut disks = probe.disks.lock().unwrap_or_else(|e| e.into_inner());
        if disks.contains(&dev) {
            return;
        }
        disks.push(dev);
    }
    let path = dir.join(".hostlag-disk-probe");
    let Ok(file) = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)
    else {
        return;
    };
    let Some(handle) = add_source(DISK_SOURCE) else {
        return;
    };
    let _ = std::thread::Builder::new()
        .name("host-lag-disk".into())
        .spawn(move || {
            let block = [0x5au8; 4096];
            let mut i = 0u64;
            while !probe.stop.load(Ordering::Relaxed) {
                std::thread::sleep(PERIOD);
                let at = mono_ns();
                let t = Instant::now();
                // Eight blocks, round and round: dirty pages are replaced,
                // not accumulated.
                let wrote = file.write_all_at(&block, (i % 8) * 4096);
                let took = t.elapsed();
                i += 1;
                if wrote.is_err() {
                    break;
                }
                handle.push(at, took);
            }
            let _ = std::fs::remove_file(&path);
        });
}

/// Start the probe for `scenario` (and forget the previous scenario's
/// inconclusive notes).
pub fn begin(scenario: &str) {
    NOTES.lock().unwrap_or_else(|e| e.into_inner()).clear();
    let probe = Arc::new(Probe {
        scenario: scenario.to_string(),
        started: mono_ns(),
        sources: Mutex::new(Vec::new()),
        injected: Mutex::new(Vec::new()),
        psi_start: Psi::read(),
        stop: AtomicBool::new(false),
        disks: Mutex::new(Vec::new()),
    });
    *CURRENT.lock().unwrap_or_else(|e| e.into_inner()) = Some(probe.clone());
    if let Some(h) = add_source("harness") {
        let p = probe.clone();
        let _ = std::thread::Builder::new()
            .name("host-lag-probe".into())
            .spawn(move || probe_loop(&p.stop, |due, late| h.push(due, late)));
    }
}

/// Stop the probe: its summary (for the results file) and the scenario's
/// inconclusive notes.
pub fn end() -> (Option<serde_json::Value>, Vec<String>) {
    let probe = CURRENT.lock().unwrap_or_else(|e| e.into_inner()).take();
    let notes = std::mem::take(&mut *NOTES.lock().unwrap_or_else(|e| e.into_inner()));
    let Some(probe) = probe else {
        return (None, notes);
    };
    probe.stop.store(true, Ordering::Relaxed);
    (Some(summary(&probe)), notes)
}

/// Record that a load injector held `node` stopped from `from` to `to`.
pub fn note_injected(node: &str, what: &str, from: Instant, to: Instant) {
    if let Some(p) = current() {
        p.injected
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Injected {
                node: node.to_string(),
                what: what.to_string(),
                from: mono_of(from),
                to: mono_of(to),
            });
    }
}

/// How many pauses of `node` an injector completed that overlap
/// `[from, to]` (zero outside a scenario or without a load profile). A
/// scenario with an exact count that a stopped daemon changes by contract
/// (a lock holder paused past its grant is outwaited by the owner, once
/// per pause) allows that many more.
pub fn injected_pauses(node: &str, from: Instant, to: Instant) -> usize {
    let Some(p) = current() else {
        return 0;
    };
    pauses_in(&p, node, mono_of(from), mono_of(to))
}

fn pauses_in(p: &Probe, node: &str, w0: u64, w1: u64) -> usize {
    p.injected
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|i| i.node == node && overlap(i.from, i.to, w0, w1) > 0)
        .count()
}

/// Record an inconclusive latency verdict for the current scenario.
pub fn inconclusive(note: String) {
    eprintln!("    INCONCLUSIVE: {note}");
    NOTES.lock().unwrap_or_else(|e| e.into_inner()).push(note);
}

/// What the host took from one window.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Lag {
    /// The most any one probe lost in the window (sum of its lateness).
    pub stolen: Duration,
    /// The longest single late wake-up in it.
    pub max: Duration,
    /// Injected pauses overlapping it (their union's overlap).
    pub injected: Duration,
    /// The source that lost the most.
    pub worst: String,
    /// What the disk-write probe lost in the window ([`add_disk_probe`]).
    /// Not part of `stolen`: it excuses a bound only under a load profile
    /// ([`Lag::excusing_disk`]); elsewhere it is reported and excuses
    /// nothing, so a daemon-caused I/O regression still fails.
    pub disk: Duration,
}

impl Lag {
    /// This lag with the disk probe counted as a source (a load campaign:
    /// the harness put the disk under load itself, or a profile's
    /// injectors did).
    pub fn excusing_disk(mut self) -> Lag {
        if self.disk > self.stolen {
            self.stolen = self.disk;
            self.worst = DISK_SOURCE.to_string();
        }
        self
    }

    /// Time the window lost to the host, all told.
    pub fn explained(&self) -> Duration {
        self.stolen + self.injected
    }
}

impl std::fmt::Display for Lag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "host lag {:?} (worst probe {:?}: longest stall {:?}; injected pauses {:?}; \
             disk-write probe lost {:?}, which excuses a bound only under a load profile)",
            self.explained(),
            self.worst,
            self.max,
            self.injected,
            self.disk
        )
    }
}

fn overlap(a0: u64, a1: u64, b0: u64, b1: u64) -> u64 {
    a1.min(b1).saturating_sub(a0.max(b0))
}

/// What the host took from `[from, to]` (zero outside a scenario).
pub fn lag_between(from: Instant, to: Instant) -> Lag {
    let Some(p) = current() else {
        return Lag::default();
    };
    let (w0, w1) = (mono_of(from), mono_of(to));
    lag_in(&p, w0, w1)
}

fn lag_in(p: &Probe, w0: u64, w1: u64) -> Lag {
    let mut lag = Lag::default();
    for s in p.sources.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        let (mut stolen, mut max) = (0u64, 0u64);
        for x in &s.samples {
            let late = x.late_us as u64 * 1_000;
            let o = overlap(x.due, x.due + late, w0, w1);
            if o > 0 {
                stolen += o;
                max = max.max(late);
            }
        }
        if s.name == DISK_SOURCE {
            lag.disk = Duration::from_nanos(stolen);
        } else if Duration::from_nanos(stolen) >= lag.stolen {
            lag.stolen = Duration::from_nanos(stolen);
            lag.worst = s.name.clone();
        }
        lag.max = lag.max.max(Duration::from_nanos(max));
    }
    // The union of the injected intervals, clipped to the window.
    let mut iv: Vec<(u64, u64)> = p
        .injected
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|i| (i.from.max(w0), i.to.min(w1)))
        .filter(|(a, b)| b > a)
        .collect();
    iv.sort_unstable();
    let (mut total, mut cur): (u64, Option<(u64, u64)>) = (0, None);
    for (a, b) in iv {
        match cur {
            Some((c0, c1)) if a <= c1 => cur = Some((c0, c1.max(b))),
            Some((c0, c1)) => {
                total += c1 - c0;
                cur = Some((a, b));
            }
            None => cur = Some((a, b)),
        }
    }
    if let Some((c0, c1)) = cur {
        total += c1 - c0;
    }
    lag.injected = Duration::from_nanos(total);
    lag
}

/// The verdict on one latency upper bound.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Within,
    /// Exceeded, and the host explains it: the note says how.
    Inconclusive(String),
    /// Exceeded with an attentive host: a latency regression.
    Exceeded(String),
}

/// No host lag excuses a measurement past `CAP_FACTOR` × its bound (or
/// the bound plus all the time the host was seen to take, if that is
/// more): "latency may degrade" is not "may hang".
pub const CAP_FACTOR: u32 = 3;

/// Judge `measured` against `bound`, of which `protocol` is the
/// protocol's own minimum and the rest host slack, given what the host
/// took from the measured window (`lag`). The rules, in order (each note
/// names the one that fired):
///
/// 1. past the cap (`max(CAP_FACTOR × bound, bound + lag.explained())`):
///    exceeded, whatever else;
/// 2. **lost time**: the probes (and injected pauses) lost at least the
///    excess in the window: inconclusive;
/// 3. without a load profile (`profile` false: a plain lane), nothing
///    else excuses: exceeded;
/// 4. **single stall**: one stall longer than the slack, and the excess
///    within one more protocol cycle plus the lost time: inconclusive;
/// 5. **probe-blind injector**: `injecting` names an applied injector the
///    probe cannot see (runtime hog, store stall, S3 or link faults):
///    inconclusive;
/// 6. otherwise exceeded.
pub fn judge(
    what: &str,
    measured: Duration,
    protocol: Duration,
    bound: Duration,
    lag: &Lag,
    profile: bool,
    injecting: Option<&str>,
) -> Verdict {
    if measured <= bound {
        return Verdict::Within;
    }
    let excess = measured - bound;
    let slack = bound.saturating_sub(protocol);
    let head =
        format!("{what}: {measured:?} > bound {bound:?} (protocol {protocol:?} + slack {slack:?})");
    let cap = (bound * CAP_FACTOR).max(bound + lag.explained());
    if measured > cap {
        return Verdict::Exceeded(format!(
            "{head}; past the hard cap {cap:?} that no host lag excuses; {lag}"
        ));
    }
    if lag.explained() >= excess {
        return Verdict::Inconclusive(format!(
            "{head}; [lost time] {lag} covers the excess {excess:?}"
        ));
    }
    if !profile {
        return Verdict::Exceeded(format!(
            "{head}; {lag} does not explain the excess {excess:?}"
        ));
    }
    if !slack.is_zero() && lag.max >= slack && excess <= protocol + lag.explained() {
        return Verdict::Inconclusive(format!(
            "{head}; [single stall] a {:?} stall exceeded the slack, the excess {excess:?} is \
             within one protocol cycle; {lag}",
            lag.max
        ));
    }
    if let Some(load) = injecting {
        return Verdict::Inconclusive(format!(
            "{head}; [probe-blind injector] `{load}` applied; {lag}"
        ));
    }
    Verdict::Exceeded(format!(
        "{head}; {lag} does not explain the excess {excess:?}"
    ))
}

/// A latency upper bound: `Ok` within it, or exceeded but explained by
/// the host (then noted as inconclusive); `Err` only when the host was
/// attentive. `protocol` is the protocol's own part of `bound` (the rest
/// is host slack); the window is `[started, started + measured]`.
pub fn latency_bound(
    what: &str,
    started: Instant,
    measured: Duration,
    protocol: Duration,
    bound: Duration,
) -> Result<()> {
    latency_bound_over(
        what,
        (started, started + measured),
        measured,
        protocol,
        bound,
    )
}

/// [`latency_bound`] for a `measured` that is not the length of one
/// window: a median or another quantile of many operations, taken over
/// `window` (what the host took from the whole of it is what could have
/// slowed them).
pub fn latency_bound_over(
    what: &str,
    window: (Instant, Instant),
    measured: Duration,
    protocol: Duration,
    bound: Duration,
) -> Result<()> {
    let injecting = crate::load::injecting();
    let profile = crate::load::profile().is_some();
    let lag = lag_between(window.0, window.1);
    let lag = if profile { lag.excusing_disk() } else { lag };
    match judge(
        what,
        measured,
        protocol,
        bound,
        &lag,
        profile,
        injecting.as_deref(),
    ) {
        Verdict::Within => Ok(()),
        Verdict::Inconclusive(note) => {
            inconclusive(note);
            Ok(())
        }
        Verdict::Exceeded(e) => anyhow::bail!("{e}"),
    }
}

// ------------------------------------------------------------ summary

/// `/proc/pressure/*` totals (µs stalled, `some` and `full`).
#[derive(Debug, Clone, Default)]
struct Psi {
    at: u64,
    cpu: (u64, u64),
    io: (u64, u64),
    memory: (u64, u64),
}

impl Psi {
    fn read() -> Psi {
        fn one(kind: &str) -> (u64, u64) {
            let text =
                std::fs::read_to_string(format!("/proc/pressure/{kind}")).unwrap_or_default();
            parse_psi(&text)
        }
        Psi {
            at: mono_ns(),
            cpu: one("cpu"),
            io: one("io"),
            memory: one("memory"),
        }
    }
}

/// `(some total, full total)` of one `/proc/pressure/*` file.
fn parse_psi(text: &str) -> (u64, u64) {
    let total = |prefix: &str| {
        text.lines()
            .find(|l| l.starts_with(prefix))
            .and_then(|l| l.split_whitespace().find_map(|f| f.strip_prefix("total=")))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    (total("some"), total("full"))
}

fn pct(stalled_us: u64, window_ns: u64) -> f64 {
    if window_ns == 0 {
        return 0.0;
    }
    (stalled_us as f64 * 1_000.0 / window_ns as f64 * 1_000.0).round() / 10.0
}

fn quantile(sorted: &[u32], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[i] as f64 / 1_000.0
}

fn source_summary(name: &str, samples: &[Sample]) -> serde_json::Value {
    let mut late: Vec<u32> = samples.iter().map(|s| s.late_us).collect();
    late.sort_unstable();
    let stolen_ms: u64 = late.iter().map(|&l| l as u64).sum::<u64>() / 1_000;
    serde_json::json!({
        "source": name,
        "samples": late.len(),
        "p50_ms": quantile(&late, 0.5),
        "p99_ms": quantile(&late, 0.99),
        "max_ms": quantile(&late, 1.0),
        "stolen_ms": stolen_ms,
    })
}

fn summary(p: &Probe) -> serde_json::Value {
    let end = Psi::read();
    let window = end.at.saturating_sub(p.psi_start.at);
    let d = |a: (u64, u64), b: (u64, u64)| {
        serde_json::json!({
            "some_pct": pct(b.0.saturating_sub(a.0), window),
            "full_pct": pct(b.1.saturating_sub(a.1), window),
        })
    };
    let sources: Vec<serde_json::Value> = p
        .sources
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|s| source_summary(&s.name, &s.samples))
        .collect();
    let injected: Vec<serde_json::Value> = p
        .injected
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|i| {
            serde_json::json!({
                "node": i.node,
                "what": i.what,
                "at_s": (i.from.saturating_sub(p.started)) as f64 / 1e9,
                "ms": (i.to.saturating_sub(i.from)) / 1_000_000,
            })
        })
        .collect();
    serde_json::json!({
        "scenario": p.scenario,
        "period_ms": PERIOD.as_millis() as u64,
        "seconds": window as f64 / 1e9,
        "probes": sources,
        "injected": injected,
        "psi": {
            "cpu": d(p.psi_start.cpu, end.cpu),
            "io": d(p.psi_start.io, end.io),
            "memory": d(p.psi_start.memory, end.memory),
        },
    })
}

// --------------------------------------------------- standalone probe

/// `harness host-lag`: with `stream`, print every sample until killed
/// (the per-cgroup probe a parent harness reads); else probe for
/// `seconds` and print the summary as JSON.
pub fn host_lag_cmd(seconds: u64, stream: bool) -> Result<()> {
    use std::io::Write;
    let stop = AtomicBool::new(false);
    if stream {
        let mut out = std::io::stdout().lock();
        let mut failed = false;
        probe_loop(&stop, |due, late| {
            if !failed
                && writeln!(out, "{due} {}", late.as_micros())
                    .and_then(|_| out.flush())
                    .is_err()
            {
                // The parent went away: stop.
                failed = true;
                stop.store(true, Ordering::Relaxed);
            }
        });
        return Ok(());
    }
    let probe = Probe {
        scenario: "host-lag".into(),
        started: mono_ns(),
        sources: Mutex::new(vec![Source {
            name: "harness".into(),
            samples: Vec::new(),
        }]),
        injected: Mutex::new(Vec::new()),
        psi_start: Psi::read(),
        stop: AtomicBool::new(false),
        disks: Mutex::new(Vec::new()),
    };
    let until = Instant::now() + Duration::from_secs(seconds.max(1));
    probe_loop(&stop, |due, late| {
        probe.sources.lock().unwrap()[0].samples.push(Sample {
            due,
            late_us: late.as_micros().min(u32::MAX as u128) as u32,
        });
        if Instant::now() >= until {
            stop.store(true, Ordering::Relaxed);
        }
    });
    println!("{}", serde_json::to_string_pretty(&summary(&probe))?);
    Ok(())
}

/// Parse one `--stream` line.
pub fn parse_stream_line(line: &str) -> Option<(u64, Duration)> {
    let mut it = line.split_whitespace();
    let due = it.next()?.parse().ok()?;
    let late: u64 = it.next()?.parse().ok()?;
    Some((due, Duration::from_micros(late)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_with(samples: Vec<Sample>, injected: Vec<Injected>) -> Probe {
        Probe {
            scenario: "t".into(),
            started: 0,
            sources: Mutex::new(vec![
                Source {
                    name: "harness".into(),
                    samples,
                },
                Source {
                    name: "cg:a".into(),
                    samples: vec![Sample {
                        due: 5_000_000_000,
                        late_us: 100_000,
                    }],
                },
            ]),
            injected: Mutex::new(injected),
            psi_start: Psi::default(),
            stop: AtomicBool::new(false),
            disks: Mutex::new(Vec::new()),
        }
    }

    #[test]
    fn pauses_are_counted_per_node_inside_the_window() {
        const S: u64 = 1_000_000_000;
        let inj = |node: &str, from, to| Injected {
            node: node.into(),
            what: "pause".into(),
            from,
            to,
        };
        let p = probe_with(
            vec![],
            vec![
                inj("b", 2 * S, 4 * S),
                inj("b", 10 * S, 12 * S),
                inj("a", 2 * S, 4 * S),
            ],
        );
        assert_eq!(pauses_in(&p, "b", 0, 20 * S), 2);
        assert_eq!(pauses_in(&p, "b", 3 * S, 5 * S), 1);
        assert_eq!(pauses_in(&p, "b", 5 * S, 9 * S), 0);
        assert_eq!(pauses_in(&p, "c", 0, 20 * S), 0);
    }

    #[test]
    fn lag_is_clipped_to_the_window_and_injected_pauses_are_a_union() {
        const S: u64 = 1_000_000_000;
        let p = probe_with(
            vec![
                // 2 s late from t=1 s: half inside [2 s, 4 s].
                Sample {
                    due: S,
                    late_us: 2_000_000,
                },
                // Outside.
                Sample {
                    due: 10 * S,
                    late_us: 500_000,
                },
            ],
            vec![
                Injected {
                    node: "a".into(),
                    what: "pause".into(),
                    from: 2 * S,
                    to: 3 * S,
                },
                Injected {
                    node: "b".into(),
                    what: "pause".into(),
                    from: 2_500_000_000,
                    to: 5 * S,
                },
            ],
        );
        let lag = lag_in(&p, 2 * S, 4 * S);
        assert_eq!(lag.stolen, Duration::from_secs(1));
        assert_eq!(lag.worst, "harness");
        assert_eq!(lag.max, Duration::from_secs(2));
        // [2, 3] ∪ [2.5, 5] clipped to [2, 4] = 2 s.
        assert_eq!(lag.injected, Duration::from_secs(2));
        assert_eq!(lag.explained(), Duration::from_secs(3));
        // The cgroup probe's 100 ms at 5 s is outside.
        let lag = lag_in(&p, 5 * S, 6 * S);
        assert_eq!(lag.stolen, Duration::from_millis(100));
        assert_eq!(lag.worst, "cg:a");
    }

    #[test]
    fn verdicts() {
        let s = Duration::from_secs;
        let lag = |stolen, max| Lag {
            stolen,
            max,
            injected: Duration::ZERO,
            worst: "harness".into(),
            disk: Duration::ZERO,
        };
        let quiet = Lag::default();
        let is_in =
            |v: Verdict, rule: &str| matches!(&v, Verdict::Inconclusive(n) if n.contains(rule));
        let exceeded = |v: Verdict| matches!(v, Verdict::Exceeded(_));
        assert_eq!(
            judge("x", s(7), s(5), s(8), &quiet, false, None),
            Verdict::Within
        );
        // Exceeded on an attentive host: a failure, profile or not.
        assert!(exceeded(judge("x", s(10), s(5), s(8), &quiet, false, None)));
        assert!(exceeded(judge("x", s(10), s(5), s(8), &quiet, true, None)));
        // The host lost more than the excess: inconclusive, in any lane.
        let starved = lag(s(3), Duration::from_millis(400));
        assert!(is_in(
            judge("x", s(10), s(5), s(8), &starved, false, None),
            "[lost time]"
        ));
        // One stall longer than the slack: only under a profile, and only
        // within one protocol cycle (+ the lost time) of the bound.
        let stalled = lag(s(1), s(4));
        assert!(exceeded(judge(
            "x",
            s(12),
            s(5),
            s(8),
            &stalled,
            false,
            None
        )));
        assert!(is_in(
            judge("x", s(12), s(5), s(8), &stalled, true, None),
            "[single stall]"
        ));
        assert!(exceeded(judge(
            "x",
            s(20),
            s(5),
            s(8),
            &stalled,
            true,
            None
        )));
        // A little lag is no excuse for a large excess.
        let little = lag(Duration::from_millis(200), Duration::from_millis(50));
        assert!(exceeded(judge("x", s(20), s(5), s(8), &little, true, None)));
        // A probe-blind injector excuses, but never past the cap (3 x
        // bound, or bound + the lost time).
        assert!(is_in(
            judge("x", s(20), s(5), s(8), &quiet, true, Some("runtime-hog")),
            "[probe-blind injector]"
        ));
        assert!(exceeded(judge(
            "x",
            s(30),
            s(0),
            s(1),
            &quiet,
            true,
            Some("s3")
        )));
        assert!(matches!(
            judge("x", s(30), s(0), s(1), &quiet, true, Some("s3")),
            Verdict::Exceeded(n) if n.contains("hard cap")
        ));
        // ... nor past it by lost time alone: 25 s lost covers a 26 s
        // measurement against 1 s (cap = 1 + 25 = 26 s), not 27 s.
        let huge = lag(s(25), s(25));
        assert!(is_in(
            judge("x", s(26), s(0), s(1), &huge, false, None),
            "[lost time]"
        ));
        assert!(exceeded(judge("x", s(27), s(0), s(1), &huge, false, None)));
    }

    #[test]
    fn psi_and_stream_lines_parse() {
        let text = "some avg10=0.00 avg60=0.00 avg300=0.00 total=1234\n\
                    full avg10=0.00 avg60=0.00 avg300=0.00 total=56\n";
        assert_eq!(parse_psi(text), (1234, 56));
        assert_eq!(parse_psi("some avg10=1 total=7\n"), (7, 0));
        assert_eq!(parse_psi(""), (0, 0));
        assert_eq!(
            parse_stream_line("123 4567\n"),
            Some((123, Duration::from_micros(4567)))
        );
        assert_eq!(parse_stream_line("x"), None);
    }

    /// The disk-write probe's lost time is reported always and excuses a
    /// bound only when the lag is taken `excusing_disk` (under a load
    /// profile): a daemon-caused I/O regression still fails a plain run.
    #[test]
    fn the_disk_probe_excuses_only_under_a_load_profile() {
        const S: u64 = 1_000_000_000;
        let sample = |due: u64, late_us: u32| Sample { due, late_us };
        let p = probe_with(Vec::new(), Vec::new());
        {
            let mut sources = p.sources.lock().unwrap();
            sources.push(Source {
                name: DISK_SOURCE.into(),
                samples: vec![sample(2 * S, 1_000_000), sample(4 * S, 1_000_000)],
            });
        }
        let lag = lag_in(&p, 0, 10 * S);
        assert_eq!(lag.disk, Duration::from_secs(2));
        assert_ne!(lag.worst, DISK_SOURCE);
        let (measured, protocol, bound) = (
            Duration::from_millis(2_200),
            Duration::from_millis(100),
            Duration::from_millis(500),
        );
        let verdict =
            |lag: &Lag, profile| judge("w", measured, protocol, bound, lag, profile, None);
        // A plain run: the 2 s the disk lost explain nothing.
        assert!(matches!(verdict(&lag, false), Verdict::Exceeded(_)));
        // A load campaign: the 2 s cover the 1.7 s excess (lost time).
        assert!(matches!(
            verdict(&lag.clone().excusing_disk(), true),
            Verdict::Inconclusive(_)
        ));
        assert_eq!(lag.clone().excusing_disk().worst, DISK_SOURCE);
    }

    /// The probe is process-global: the tests that run one take turns.
    static SERIAL: Mutex<()> = Mutex::new(());

    #[test]
    fn the_probe_measures_its_own_sleep() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        begin("probe-test");
        std::thread::sleep(Duration::from_millis(300));
        let lag = lag_between(Instant::now() - Duration::from_millis(250), Instant::now());
        let (summary, notes) = end();
        let summary = summary.unwrap();
        assert!(notes.is_empty());
        assert_eq!(summary["probes"][0]["source"], "harness");
        assert!(
            summary["probes"][0]["samples"].as_u64().unwrap() >= 3,
            "{summary}"
        );
        // What a window lost is clipped to it.
        assert!(lag.stolen <= Duration::from_millis(250), "{lag:?}");
        assert_eq!(lag.injected, Duration::ZERO);
    }

    #[test]
    fn the_disk_probe_samples_writes_and_stops_with_the_scenario() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        begin("disk-probe-test");
        add_disk_probe(dir.path());
        // The same device again: one probe.
        add_disk_probe(dir.path());
        std::thread::sleep(PERIOD * 6);
        let probe = current().expect("a probe");
        let sources = probe.sources.lock().unwrap();
        let disks: Vec<_> = sources.iter().filter(|s| s.name == "disk").collect();
        assert_eq!(disks.len(), 1, "one disk source per device");
        assert!(!disks[0].samples.is_empty(), "no write was timed");
        drop(sources);
        drop(probe);
        let _ = end();
    }
}
