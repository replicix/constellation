//! Load injectors for the host-load campaign (phase 0): what a starved,
//! paused, throttled or slow host does to the daemons, applied to every
//! daemon the harness spawns.
//!
//! `CONSTELLATION_HARNESS_LOAD=<item>[,<item>...]` selects them, each item
//! a preset name or `kind[:arg...][@node]` (`@node` limits it to the
//! clients of that name; without it every daemon gets it):
//!
//! | item | what |
//! |---|---|
//! | `cpu:<pct>` / `cpu:bursty[:<hi>:<lo>:<period_ms>]` | cgroup v2 `cpu.max` on a per-daemon cgroup (percent of one CPU, as systemd's `CPUQuota`); bursty toggles 100 % / 2 % every 3 s |
//! | `hogs[:cpu=N][:load=P][:vm=N][:vmbytes=S][:io=N][:hdd=N][:hddbytes=S]` | `stress-ng` beside the scenario (its own process group, killed at the end); `hogs` alone is every stressor at the spec's sizes |
//! | `pause:<dur>[:<every_ms>]` | `SIGSTOP`/`SIGCONT` of the daemon for `<dur>` every `<every_ms>` (default 15 s) |
//! | `step-pause:<dur>[:<every_ms>]` | the same, but the daemon stops itself mid-step (`CONSTELLATION_FAULT_STEP_PAUSE_FILE`) |
//! | `store-stall:<ms>[:<every_ms>]` | one metadata write holds the writer lock `<ms>` every `<every_ms>` (`CONSTELLATION_FAULT_STORE_STALL_FILE`) |
//! | `log-stall:<ms>[:<every_ms>]` | the daemon's log sink stalls `<ms>` (one write, holding std's stderr lock) every `<every_ms>` (`CONSTELLATION_FAULT_LOG_STALL_FILE`). Not probe-blind: the daemon's log writer never blocks a thread, so a log stall must cost the workload no latency and the latency bounds apply |
//! | `runtime-hog:<n>:<ms>[:<gap_ms>]` | `CONSTELLATION_FAULT_RUNTIME_HOG` |
//! | `s3:latency:<ms>:<jitter>` / `s3:bandwidth:<kBps>` / `s3:slicer:<bytes>:<delay_us>` / `s3:blackhole:<ms>[:<at_ms>]` | toxics on the S3 path of every client |
//! | `net:<netem args...>` / `net:flap[:<netem args...>]` | `tc netem` on the daemon's own P2P (UDP) ports on `lo`; flap: loss 100 % for 2 s every 10 s |
//! | `clock:<±ms>` / `clock:spread:<ms>` | `CONSTELLATION_FAULT_CLOCK_OFFSET_MS` (within ±500 ms); spread alternates `+ms`, `-ms` across nodes |
//! | `clock:step:<±ms>:<at_ms>[@node]` | at `<at_ms>` after the daemon spawned, the daemon's protocol wall clock steps by `<ms>` (`CONSTELLATION_FAULT_CLOCK_STEP_FILE`; plan 43 C7-H1): the honour clock follows a forward step, a backward one and the wait clock move nothing; the steps are cumulative per node, armed once per node and kept across a daemon restart (a restarted daemon starts stepped; the step is not applied again) |
//! | `clock:rate:<±ppm>[:unbudgeted][@node]` | the daemon's boot, raw and monotonic clocks run at `1 + ppm/1e6` of the host's rate from its start, its wall does not (`CONSTELLATION_FAULT_CLOCK_RATE_PPM`; plan 43 C7a): an oscillator off by `ppm` under a disciplined wall. Unless `:unbudgeted`, the node's rate budget is raised to cover it (`CONSTELLATION_CLOCK_RATE_BUDGET_PPM = max(1000, |ppm|)`; each node budgets its own oscillator); `|ppm| < 500000`. The budget is carried from C7a's first commit and read by the clocks from its second |
//!
//! `<dur>` is milliseconds or a protocol boundary with an optional offset
//! (`lock-lapse+500`, `takeover-300`, `lease-ttl+1000`: see
//! [`crate::timings::Timings::boundaries`]), computed from the paused
//! daemon's own environment. Presets: [`PRESETS`].
//!
//! Root is needed for `cpu` (cgroups) and `net` (`tc`): the harness runs
//! those through `sudo -n` (directly as root) and fails the run at its
//! start when it cannot. Everything it creates is removed when the
//! scenario ends, and a crashed run's cgroups are swept by the next
//! (`tools/load-campaign/inject.sh cleanup` removes the rest by hand).
//!
//! Why not per-daemon network namespaces (the review's first idea): a
//! daemon started under `ip netns exec` gets a private mount namespace, so
//! its FUSE mount is invisible to the harness; it would run as root under
//! `sudo` (the harness's pid, kill and pause bookkeeping would be the
//! wrapper's); and the S3 endpoint is a loopback port that another
//! network namespace cannot reach without NAT. Filtering `lo` by the
//! daemon's own UDP ports gives the same per-daemon peer-link delay with
//! none of that, and leaves its S3 traffic (TCP, through toxiproxy) to the
//! `s3:` items.
//!
//! Every daemon spawned under a profile also gets the hang watch's
//! settings when verification is on ([`crate::verify`]).

use crate::timings::Timings;
use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The variable that selects the profile.
pub const LOAD_ENV: &str = "CONSTELLATION_HARNESS_LOAD";

/// Named profiles (load campaign §5.3-§5.5).
pub const PRESETS: &[(&str, &str)] = &[
    ("baseline", ""),
    ("cpu25", "cpu:25"),
    ("cpu10", "cpu:10"),
    ("cpu-bursty", "cpu:bursty"),
    ("hogs", "hogs"),
    ("runtime-hog", "runtime-hog:4:200"),
    ("store-stall5", "store-stall:5000:15000"),
    ("store-stall20", "store-stall:20000:40000"),
    ("log-stall12", "log-stall:12000:20000"),
    ("step-pause", "step-pause:lock-lapse+500:20000"),
    ("pause-takeover", "pause:takeover+300:15000"),
    ("pause-lock-lapse", "pause:lock-lapse+500:20000"),
    ("s3-jitter", "s3:latency:50:40"),
    ("s3-jitter-high", "s3:latency:500:400"),
    ("s3-blackhole", "s3:blackhole:35000:10000"),
    ("s3-slow", "s3:bandwidth:64"),
    ("net-lan", "net:delay:1ms"),
    ("net-wan", "net:delay:60ms:20ms"),
    ("net-lossy", "net:delay:30ms:20ms:loss:0.5%:reorder:5%"),
    ("net-flap", "net:flap"),
    ("clock-skew", "clock:spread:400"),
];

/// Which daemons an item applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    All,
    Node(String),
}

impl Target {
    fn matches(&self, node: &str) -> bool {
        match self {
            Target::All => true,
            Target::Node(n) => n == node,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cpu {
    /// `cpu.max` quota, percent of one CPU.
    Quota(u32),
    Bursty {
        hi: u32,
        lo: u32,
        period: Duration,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Hogs {
    pub cpu: u32,
    pub cpu_load: u32,
    pub vm: u32,
    pub vm_bytes: String,
    pub io: u32,
    pub hdd: u32,
    pub hdd_bytes: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum S3 {
    Latency { ms: u64, jitter: u64 },
    Bandwidth { kbps: u64 },
    Slicer { bytes: u64, delay_us: u64 },
    Blackhole { ms: u64, at: Duration },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    Cpu(Cpu),
    Hogs(Hogs),
    /// A pause of a spec'd length (resolved per daemon) every `every`;
    /// `step`: the daemon stops itself mid-step.
    Pause {
        dur: String,
        every: Duration,
        step: bool,
    },
    StoreStall {
        ms: u64,
        every: Duration,
    },
    /// The daemon's log sink stalls `ms` (one write) every `every`
    /// (plan 44).
    LogStall {
        ms: u64,
        every: Duration,
    },
    RuntimeHog(String),
    S3(S3),
    Net {
        netem: Vec<String>,
        flap: bool,
    },
    Clock {
        offset: i64,
    },
    ClockSpread {
        ms: i64,
    },
    /// Step the daemon's wall clock by `ms` at `at` after it spawned
    /// (plan 43 C7-H1).
    ClockStep {
        ms: i64,
        at: Duration,
    },
    /// The daemon's elapsed clocks run `ppm` fast (plan 43 C7a), budgeted
    /// for it unless `budgeted` is false.
    ClockRate {
        ppm: i64,
        budgeted: bool,
    },
}

/// The rate budget a daemon has without `clock:rate` (ppm): the platform's
/// default where the rate source is not slewed.
const DEFAULT_RATE_BUDGET_PPM: i64 = 1_000;

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub kind: Kind,
    pub target: Target,
}

/// A parsed profile.
#[derive(Debug, Clone, PartialEq)]
pub struct Profile {
    /// What was asked for, verbatim.
    pub spec: String,
    pub items: Vec<Item>,
}

fn num<T: std::str::FromStr>(s: Option<&str>, what: &str) -> Result<T> {
    let s = s.with_context(|| format!("{what}: missing"))?;
    s.trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("{what}: {s:?} is not a number"))
}

fn ms_arg(s: Option<&str>, default: u64, what: &str) -> Result<Duration> {
    Ok(Duration::from_millis(match s {
        None => default,
        some => num(some, what)?,
    }))
}

/// Parse a profile (presets expanded).
pub fn parse(spec: &str) -> Result<Profile> {
    let mut items = Vec::new();
    for raw in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (body, target) = split_target(raw);
        // A preset, optionally aimed at one node (`runtime-hog@b`).
        if let Some((_, expansion)) = PRESETS.iter().find(|(n, _)| *n == body) {
            for e in expansion
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                let (eb, et) = split_target(e);
                let t = if et == Target::All {
                    target.clone()
                } else {
                    et
                };
                items.push(parse_item(eb, t, spec)?);
            }
            continue;
        }
        items.push(parse_item(body, target, spec)?);
    }
    Ok(Profile {
        spec: spec.to_string(),
        items,
    })
}

fn split_target(raw: &str) -> (&str, Target) {
    match raw.rsplit_once('@') {
        Some((b, n)) if !n.is_empty() => (b, Target::Node(n.to_string())),
        _ => (raw, Target::All),
    }
}

/// One item (no preset).
fn parse_item(body: &str, target: Target, spec: &str) -> Result<Item> {
    let mut a = body.split(':');
    let kind = a.next().unwrap_or_default();
    let rest: Vec<&str> = a.collect();
    let arg = |i: usize| rest.get(i).copied();
    let kind = match kind {
        "cpu" => match arg(0) {
            Some("bursty") => Kind::Cpu(Cpu::Bursty {
                hi: arg(1)
                    .map(|_| num(arg(1), "cpu:bursty hi"))
                    .transpose()?
                    .unwrap_or(100),
                lo: arg(2)
                    .map(|_| num(arg(2), "cpu:bursty lo"))
                    .transpose()?
                    .unwrap_or(2),
                period: ms_arg(arg(3), 3_000, "cpu:bursty period")?,
            }),
            pct => {
                let pct: u32 = num(pct, "cpu:<pct>")?;
                anyhow::ensure!(pct > 0, "cpu:<pct> must be positive");
                Kind::Cpu(Cpu::Quota(pct))
            }
        },
        "hogs" => {
            let nproc = std::thread::available_parallelism().map_or(4, |n| n.get()) as u32;
            let mut h = Hogs {
                cpu: nproc,
                cpu_load: 90,
                vm: 4,
                vm_bytes: "60%".into(),
                io: 4,
                hdd: 2,
                hdd_bytes: "256m".into(),
            };
            if !rest.is_empty() {
                // Only the stressors named.
                h.cpu = 0;
                h.vm = 0;
                h.io = 0;
                h.hdd = 0;
            }
            for r in &rest {
                let (k, v) = r.split_once('=').unwrap_or((r, ""));
                let n = |dflt: u32| -> Result<u32> {
                    if v.is_empty() {
                        Ok(dflt)
                    } else {
                        num(Some(v), &format!("hogs:{k}"))
                    }
                };
                match k {
                    "cpu" => h.cpu = n(nproc)?,
                    "load" => h.cpu_load = n(90)?,
                    "vm" => h.vm = n(4)?,
                    "vmbytes" => h.vm_bytes = v.to_string(),
                    "io" => h.io = n(4)?,
                    "hdd" => h.hdd = n(2)?,
                    "hddbytes" => h.hdd_bytes = v.to_string(),
                    other => bail!("hogs: unknown stressor {other:?}"),
                }
            }
            anyhow::ensure!(
                h.cpu + h.vm + h.io + h.hdd > 0,
                "hogs: no stressor selected"
            );
            Kind::Hogs(h)
        }
        "pause" | "step-pause" => {
            let dur = arg(0).context("pause:<dur>: missing")?.to_string();
            // Must resolve against the defaults at least.
            Timings::from_pairs::<&str, &str>(&[])
                .duration_of(&dur)
                .with_context(|| format!("pause:{dur}: not a duration or boundary"))?;
            Kind::Pause {
                dur,
                every: ms_arg(arg(1), 15_000, "pause every")?,
                step: kind == "step-pause",
            }
        }
        "store-stall" => Kind::StoreStall {
            ms: num(arg(0), "store-stall:<ms>")?,
            every: ms_arg(arg(1), 15_000, "store-stall every")?,
        },
        "log-stall" => Kind::LogStall {
            ms: num(arg(0), "log-stall:<ms>")?,
            every: ms_arg(arg(1), 15_000, "log-stall every")?,
        },
        "runtime-hog" => {
            anyhow::ensure!(
                (2..=3).contains(&rest.len())
                    && rest.iter().all(|r| r.parse::<u64>().is_ok_and(|v| v > 0)),
                "runtime-hog:<n>:<ms>[:<gap_ms>]: got {body:?}"
            );
            Kind::RuntimeHog(rest.join(":"))
        }
        "s3" => Kind::S3(match arg(0) {
            Some("latency") => S3::Latency {
                ms: num(arg(1), "s3:latency ms")?,
                jitter: arg(2)
                    .map(|_| num(arg(2), "s3:latency jitter"))
                    .transpose()?
                    .unwrap_or(0),
            },
            Some("bandwidth") => S3::Bandwidth {
                kbps: num(arg(1), "s3:bandwidth kBps")?,
            },
            Some("slicer") => S3::Slicer {
                bytes: num(arg(1), "s3:slicer bytes")?,
                delay_us: num(arg(2), "s3:slicer delay_us")?,
            },
            Some("blackhole") => S3::Blackhole {
                ms: num(arg(1), "s3:blackhole ms")?,
                at: ms_arg(arg(2), 10_000, "s3:blackhole at")?,
            },
            other => bail!("s3:{other:?}: expected latency, bandwidth, slicer or blackhole"),
        }),
        "net" => {
            let flap = arg(0) == Some("flap");
            let netem: Vec<String> = rest
                .iter()
                .skip(usize::from(flap))
                .map(|s| s.to_string())
                .collect();
            anyhow::ensure!(flap || !netem.is_empty(), "net: no netem arguments");
            Kind::Net { netem, flap }
        }
        "clock" => match arg(0) {
            Some("spread") => Kind::ClockSpread {
                ms: num(arg(1), "clock:spread ms")?,
            },
            // The node is the item's `@node` (the last `@`), not part of
            // the step's own fields.
            Some("step") => {
                let ms: i64 = num(arg(1), "clock:step:<ms>")?;
                anyhow::ensure!(ms != 0, "clock:step:0 steps nothing");
                Kind::ClockStep {
                    ms,
                    at: ms_arg(arg(2), 0, "clock:step:<ms>:<at_ms>")?,
                }
            }
            Some("rate") => {
                let ppm: i64 = num(arg(1), "clock:rate:<ppm>")?;
                anyhow::ensure!(ppm != 0, "clock:rate:0 skews nothing");
                anyhow::ensure!(
                    ppm.abs() < 500_000,
                    "clock:rate:{ppm}: the rate fault is refused from 500000 ppm"
                );
                let budgeted = match arg(2) {
                    None => true,
                    Some("unbudgeted") => false,
                    Some(other) => bail!("clock:rate:{ppm}:{other}: expected `unbudgeted`"),
                };
                Kind::ClockRate { ppm, budgeted }
            }
            v => {
                let offset: i64 = num(v, "clock:<ms>")?;
                Kind::Clock { offset }
            }
        },
        other => bail!(
            "unknown load item {other:?} in {spec:?} (presets: {})",
            PRESETS
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    if let Kind::Clock { offset } = kind {
        anyhow::ensure!(
            offset.abs() <= 500,
            "clock:{offset}: outside the ±500 ms the design assumes"
        );
    }
    if let Kind::ClockSpread { ms } = kind {
        anyhow::ensure!(
            (0..=500).contains(&ms),
            "clock:spread:{ms}: outside the ±500 ms the design assumes"
        );
    }
    Ok(Item { kind, target })
}

/// The run's profile (`None`: no load, or the baseline).
pub fn profile() -> Option<&'static Profile> {
    static P: OnceLock<Option<Profile>> = OnceLock::new();
    P.get_or_init(|| {
        let spec = std::env::var(LOAD_ENV).ok()?;
        // `init` validated it.
        let p = parse(&spec).ok()?;
        (!p.items.is_empty()).then_some(p)
    })
    .as_ref()
}

/// Whether the profile pauses `node` by name for a spec'd length
/// (`pause:<dur>@<node>`; a `step-pause` stops the daemon mid-step for
/// 2 s steps that may stay under any seal window, and an item aimed at
/// every node pauses the whole cluster).
pub fn pauses_node(node: &str) -> bool {
    profile().is_some_and(|p| pauses(p, node))
}

fn pauses(p: &Profile, node: &str) -> bool {
    p.items.iter().any(|i| {
        matches!(i.kind, Kind::Pause { step: false, .. })
            && matches!(&i.target, Target::Node(n) if n == node)
    })
}

/// Whether the profile stalls the daemons' log sinks (a `log-stall` item):
/// a handover then may find lines its old image had not written
/// (`unwritten at its exec`).
pub fn stalls_log() -> bool {
    profile().is_some_and(|p| {
        p.items
            .iter()
            .any(|i| matches!(i.kind, Kind::LogStall { .. }))
    })
}

/// `CONSTELLATION_HARNESS_DEADLINE_SCALE` (default 1): the factor the
/// campaign stretches liveness deadlines by under load.
pub fn deadline_scale() -> f64 {
    static S: OnceLock<f64> = OnceLock::new();
    *S.get_or_init(|| {
        std::env::var("CONSTELLATION_HARNESS_DEADLINE_SCALE")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 1.0)
            .unwrap_or(1.0)
    })
}

/// A liveness deadline, stretched by [`deadline_scale`].
pub fn scaled(d: Duration) -> Duration {
    d.mul_f64(deadline_scale())
}

/// The probe-blind injectors (runtime hog, store stall, S3 and link
/// faults) this scenario actually applied to a daemon, for
/// [`crate::hostlag::judge`]: CPU throttling and pauses are measured (the
/// cgroup probes, the injected intervals) and a clock offset adds no
/// latency, so they never excuse an exceeded bound by themselves.
pub fn injecting() -> Option<String> {
    let s = SESSION.lock().unwrap_or_else(|e| e.into_inner());
    let applied = s
        .as_ref()?
        .applied
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    (!applied.is_empty()).then(|| applied.join(", "))
}

/// The store stalls armed so far, as `(armed, armed + length)`: a
/// daemon taking an armed stall (it stalls its next ordinary write) is
/// held that long from about the arming, whichever node it is.
static STORE_STALLS: Mutex<Vec<(Instant, Instant)>> = Mutex::new(Vec::new());

/// The store stalls armed at or after `since`, from [`STORE_STALLS`]
/// (the intervals a scenario's own measurements may be excused by, or
/// judged against, while a `store-stall` item runs).
pub fn store_stalls_since(since: Instant) -> Vec<(Instant, Instant)> {
    STORE_STALLS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .copied()
        .filter(|(armed, _)| *armed >= since)
        .collect()
}

/// Note that a probe-blind injector applied (see [`injecting`]).
fn note_applied(what: String) {
    if let Some(s) = SESSION.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
        let mut a = s.applied.lock().unwrap_or_else(|e| e.into_inner());
        if !a.contains(&what) {
            a.push(what);
        }
    }
}

/// The profile's name for the results file.
pub fn label() -> Option<String> {
    std::env::var(LOAD_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Validate the profile and what it needs, once, before any scenario.
pub fn init() -> Result<()> {
    let Ok(spec) = std::env::var(LOAD_ENV) else {
        return Ok(());
    };
    let p = parse(&spec).with_context(|| format!("{LOAD_ENV}={spec:?}"))?;
    let needs_root = p
        .items
        .iter()
        .any(|i| matches!(i.kind, Kind::Cpu(_) | Kind::Net { .. }));
    if needs_root {
        root_sh("true").context(
            "the load profile needs root (cgroups, tc): run as root or with passwordless sudo",
        )?;
        sweep_cgroups();
        sweep_netem();
    }
    sweep_stopped();
    if p.items.iter().any(|i| matches!(i.kind, Kind::Net { .. })) {
        root_sh("command -v tc >/dev/null").context("the net: items need `tc` (iproute-tc)")?;
    }
    if p.items.iter().any(|i| matches!(i.kind, Kind::Hogs(_))) && !crate::suites::have("stress-ng")
    {
        bail!("the hogs item needs stress-ng");
    }
    if p.items.iter().any(|i| matches!(i.kind, Kind::Cpu(_)))
        && !Path::new("/sys/fs/cgroup/cgroup.controllers").exists()
    {
        bail!("the cpu: items need cgroup v2 at /sys/fs/cgroup");
    }
    if !p.items.is_empty() {
        eprintln!("=== load profile {spec:?}: {} item(s)", p.items.len());
    }
    Ok(())
}

// --------------------------------------------------------------- root

/// `sh -c script` as root (`sudo -n` unless already root): its stdout.
pub fn root_sh(script: &str) -> Result<String> {
    // SAFETY: no preconditions.
    let root = unsafe { libc::geteuid() } == 0;
    let mut cmd = if root {
        Command::new("sh")
    } else {
        let mut c = Command::new("sudo");
        c.arg("-n").arg("sh");
        c
    };
    let out = cmd
        .arg("-c")
        .arg(script)
        .stdin(Stdio::null())
        .output()
        .context("running a root helper")?;
    if !out.status.success() {
        bail!(
            "{script:?} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

// ------------------------------------------------------------ daemons

/// A daemon the harness spawned.
#[derive(Debug, Clone)]
pub struct Daemon {
    pub node: String,
    pub pid: u32,
    pub state: PathBuf,
    pub mnt: PathBuf,
    pub scenario: String,
    /// The protocol clock offset it was given (ms).
    pub clock_offset: i64,
}

static DAEMONS: Mutex<Vec<Daemon>> = Mutex::new(Vec::new());
/// Daemons a scenario froze itself (`Client::pause`): an injector never
/// thaws or freezes them, and the hang watch leaves them be.
static SCENARIO_PAUSED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// The daemons spawned so far that are still alive.
pub fn daemons() -> Vec<Daemon> {
    let mut d = DAEMONS.lock().unwrap_or_else(|e| e.into_inner());
    d.retain(|x| alive(x.pid));
    d.clone()
}

/// Every daemon of `scenario` ever spawned, dead or alive.
pub fn daemons_of(scenario: &str) -> Vec<Daemon> {
    ALL_DAEMONS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|d| d.scenario == scenario)
        .cloned()
        .collect()
}

static ALL_DAEMONS: Mutex<Vec<Daemon>> = Mutex::new(Vec::new());

/// `Client::pause`/`resume` say so.
pub fn note_scenario_pause(pid: u32, paused: bool) {
    let mut p = SCENARIO_PAUSED.lock().unwrap_or_else(|e| e.into_inner());
    p.retain(|x| *x != pid);
    if paused {
        p.push(pid);
    }
}

pub fn scenario_paused(pid: u32) -> bool {
    SCENARIO_PAUSED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&pid)
}

/// Whether `pid` is a `constellation` daemon (`/proc/<pid>/comm`): a pid
/// from a dead run's list may since name another process.
fn is_daemon(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/comm")).is_ok_and(|c| c.trim() == "constellation")
}

/// The process exists and is not a zombie.
pub fn alive(pid: u32) -> bool {
    proc_state(pid).is_some_and(|s| s != 'Z' && s != 'X')
}

/// `/proc/<pid>/stat`'s state letter.
pub fn proc_state(pid: u32) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(')')?.1.trim_start().chars().next()
}

fn signal(pid: u32, sig: libc::c_int) {
    // SAFETY: kill(2) of a pid this harness spawned.
    unsafe { libc::kill(pid as i32, sig) };
}

fn items_for(node: &str) -> Vec<&'static Item> {
    profile()
        .map(|p| p.items.iter().filter(|i| i.target.matches(node)).collect())
        .unwrap_or_default()
}

/// The clock offset `node` runs with under the profile.
fn clock_offset_for(node: &str) -> i64 {
    let mut offset = 0;
    for i in items_for(node) {
        match i.kind {
            Kind::Clock { offset: o } => offset = o,
            Kind::ClockSpread { ms } => {
                // +ms, -ms, +ms, ... in the order nodes are first seen.
                static ORDER: Mutex<Vec<String>> = Mutex::new(Vec::new());
                let mut order = ORDER.lock().unwrap_or_else(|e| e.into_inner());
                let idx = match order.iter().position(|n| n == node) {
                    Some(i) => i,
                    None => {
                        order.push(node.to_string());
                        order.len() - 1
                    }
                };
                offset = if idx % 2 == 0 { ms } else { -ms };
            }
            _ => {}
        }
    }
    offset
}

fn load_dir(work: &Path) -> PathBuf {
    work.join("load")
}

/// Environment for the daemon `node` is about to spawn, over the
/// client's own `env` (a scenario's `RUST_LOG` extended, not replaced).
pub fn daemon_env(node: &str, work: &Path, env: &[(String, String)], cmd: &mut Command) {
    let _ = std::fs::create_dir_all(load_dir(work));
    for i in items_for(node) {
        match &i.kind {
            Kind::RuntimeHog(spec) => {
                cmd.env("CONSTELLATION_FAULT_RUNTIME_HOG", spec);
                note_applied(format!("runtime-hog:{spec}@{node}"));
            }
            Kind::Pause { step: true, .. } => {
                cmd.env(
                    "CONSTELLATION_FAULT_STEP_PAUSE_FILE",
                    load_dir(work).join("step-pause"),
                );
            }
            Kind::StoreStall { .. } => {
                cmd.env(
                    "CONSTELLATION_FAULT_STORE_STALL_FILE",
                    load_dir(work).join("store-stall"),
                );
            }
            Kind::LogStall { .. } => {
                cmd.env(
                    "CONSTELLATION_FAULT_LOG_STALL_FILE",
                    load_dir(work).join("log-stall"),
                );
            }
            Kind::ClockRate { ppm, budgeted } => {
                cmd.env("CONSTELLATION_FAULT_CLOCK_RATE_PPM", ppm.to_string());
                if *budgeted {
                    cmd.env(
                        "CONSTELLATION_CLOCK_RATE_BUDGET_PPM",
                        DEFAULT_RATE_BUDGET_PPM.max(ppm.abs()).to_string(),
                    );
                }
            }
            _ => {}
        }
    }
    let offset = clock_offset_for(node);
    if offset != 0 {
        cmd.env("CONSTELLATION_FAULT_CLOCK_OFFSET_MS", offset.to_string());
    }
    // Every daemon can have its wall clock stepped (a scenario's
    // [`step_clock`], or a `clock:step` item); unused it is one poll of a
    // missing file every 100 ms. The file keeps the node's cumulative step,
    // so a restarted daemon starts where the host clock is.
    cmd.env(
        "CONSTELLATION_FAULT_CLOCK_STEP_FILE",
        clock_step_file(work, node),
    );
    crate::verify::daemon_env(env, cmd);
}

/// The file `node`'s daemon polls for its cumulative wall-clock step.
fn clock_step_file(work: &Path, node: &str) -> PathBuf {
    load_dir(work).join(format!("clock-step-{node}"))
}

/// One applied wall-clock step of a node's daemon: when the daemon applied
/// it (host unix µs, from the marker file's mtime) and the node's
/// cumulative step from then on.
#[derive(Debug, Clone)]
struct StepRec {
    scenario: String,
    node: String,
    at_us: i64,
    cumulative_ms: i64,
}

/// The steps applied so far, in order (the load's ledger of clock steps).
static CLOCK_STEPS: Mutex<Vec<StepRec>> = Mutex::new(Vec::new());

/// The node's cumulative step now (ms): what its daemon's protocol clock
/// reads above the host's, apart from the constant offset.
pub fn clock_step_now(node: &str) -> i64 {
    CLOCK_STEPS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .rev()
        .find(|r| r.node == node && r.scenario == crate::client::scenario())
        .map_or(0, |r| r.cumulative_ms)
}

/// The steps of `node`'s daemon in `scenario`, oldest first: `(applied at
/// host unix µs, cumulative ms from then on)`.
pub fn clock_steps_of(scenario: &str, node: &str) -> Vec<(i64, i64)> {
    CLOCK_STEPS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|r| r.scenario == scenario && r.node == node)
        .map(|r| (r.at_us, r.cumulative_ms))
        .collect()
}

/// After a step: the daemon's `status.clock` counts it (plan 43 C7-H1). A
/// step the protocol clock did not see (the file applied, the clock not
/// moved) would make every scenario that relies on it vacuous, so this is
/// part of the injector: it reads `node.status` until the daemon's
/// `steps_forward` or `steps_backward` has grown by this step (a step under
/// the detection threshold of 100 ms is not counted by the clock and not
/// waited for) and prints the counters.
fn check_step_counted(d: &Daemon, delta_ms: i64) -> Result<()> {
    // Per daemon process: how many steps of each direction it was given.
    static GIVEN: Mutex<Vec<(u32, u64, u64)>> = Mutex::new(Vec::new());
    let (fwd, bwd) = {
        let mut g = GIVEN.lock().unwrap_or_else(|e| e.into_inner());
        let slot = match g.iter().position(|(pid, _, _)| *pid == d.pid) {
            Some(i) => i,
            None => {
                g.push((d.pid, 0, 0));
                g.len() - 1
            }
        };
        if delta_ms.abs() >= 100 {
            if delta_ms > 0 {
                g[slot].1 += 1;
            } else {
                g[slot].2 += 1;
            }
        }
        (g[slot].1, g[slot].2)
    };
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let status = crate::client::control_call_at(
            &d.state,
            "node.status",
            serde_json::json!({}),
            Duration::from_secs(10),
        )?;
        let clock = &status["clock"];
        let n = |k: &str| clock[k].as_u64().unwrap_or(0);
        if n("steps_forward") >= fwd && n("steps_backward") >= bwd {
            eprintln!(
                "    load: {} (pid {}) wall clock stepped by {delta_ms} ms: status.clock steps_forward {} \
                 steps_backward {} stepped_ms_total {} last_step_ms {} honour_ahead_ms {} wait_behind_ms {}",
                d.node,
                d.pid,
                n("steps_forward"),
                n("steps_backward"),
                n("stepped_ms_total"),
                clock["last_step_ms"].as_i64().unwrap_or(0),
                clock["honour_ahead_ms"].as_i64().unwrap_or(0),
                clock["wait_behind_ms"].as_i64().unwrap_or(0),
            );
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < until,
            "{}'s status.clock did not count the {delta_ms} ms step within 10 s (wanted {fwd} \
             forward and {bwd} backward): {clock}",
            d.node
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Step `node`'s daemon's wall clock by `delta_ms` now and wait until the
/// daemon has applied it (its `.applied` marker names the new cumulative
/// step); the step goes in the ledger [`clock_steps_of`] reads. For a
/// scenario that steps a clock by hand; the `clock:step` item does the same
/// at its time. `file` is the daemon's `CONSTELLATION_FAULT_CLOCK_STEP_FILE`
/// ([`clock_step_file`], under the scenario's work directory).
fn apply_clock_step(scenario: &str, node: &str, file: &Path, delta_ms: i64) -> Result<()> {
    // Cumulative across the node's items, and across its restarts.
    static CUMULATIVE: Mutex<Vec<(String, String, i64)>> = Mutex::new(Vec::new());
    let total = {
        let mut c = CUMULATIVE.lock().unwrap_or_else(|e| e.into_inner());
        let slot = match c.iter().position(|(sc, n, _)| sc == scenario && n == node) {
            Some(i) => i,
            None => {
                c.push((scenario.to_string(), node.to_string(), 0));
                c.len() - 1
            }
        };
        c[slot].2 += delta_ms;
        c[slot].2
    };
    // Whole, by a rename: a daemon reading a prefix would step wrongly.
    let tmp = file.with_extension("tmp");
    std::fs::write(&tmp, total.to_string()).context("writing the clock step")?;
    std::fs::rename(&tmp, file).context("publishing the clock step")?;
    let mut marker = file.as_os_str().to_os_string();
    marker.push(".applied");
    let marker = PathBuf::from(marker);
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let applied = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|t| t.trim().parse::<i64>().ok());
        if applied == Some(total) {
            break;
        }
        anyhow::ensure!(
            Instant::now() < until,
            "{node}'s daemon did not apply the clock step to {total} ms within 10 s"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let at_us = std::fs::metadata(&marker)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_micros() as i64);
    CLOCK_STEPS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(StepRec {
            scenario: scenario.to_string(),
            node: node.to_string(),
            at_us,
            cumulative_ms: total,
        });
    Ok(())
}

/// The S3 endpoint a client given `endpoint` uses: a direct (proxy-less)
/// endpoint goes through the load proxy while an `s3:` item is active.
pub fn endpoint_for(endpoint: &str) -> String {
    S3_REWRITE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|m| m.get(endpoint).cloned())
        .unwrap_or_else(|| endpoint.to_string())
}

static S3_REWRITE: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

/// A daemon was spawned: place it (cgroup, netem) and start its
/// injectors.
pub fn daemon_spawned(
    node: &str,
    pid: u32,
    state: &Path,
    mnt: &Path,
    work: &Path,
    env: &[(String, String)],
) {
    let d = Daemon {
        node: node.to_string(),
        pid,
        state: state.to_path_buf(),
        mnt: mnt.to_path_buf(),
        scenario: crate::client::scenario(),
        clock_offset: clock_offset_for(node),
    };
    DAEMONS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(d.clone());
    ALL_DAEMONS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(d.clone());
    let items = items_for(node);
    if items.is_empty() {
        return;
    }
    let timings = Timings::from_pairs(env);
    let mut s = SESSION.lock().unwrap_or_else(|e| e.into_inner());
    let Some(session) = s.as_mut() else {
        return;
    };
    session.spawned.insert(node.to_string());
    for i in items {
        let r = match &i.kind {
            Kind::Cpu(cpu) => session.place_cpu(&d, cpu),
            Kind::Pause { dur, every, step } => {
                let dur = timings
                    .duration_of(dur)
                    .unwrap_or(Duration::from_millis(1_000));
                session.spawn_pauser(
                    &d,
                    dur,
                    *every,
                    step.then(|| load_dir(work).join("step-pause")),
                );
                Ok(())
            }
            Kind::StoreStall { ms, every } => {
                session.spawn_store_stall(&d, *ms, *every, load_dir(work).join("store-stall"));
                let what = format!("store-stall:{ms}@{node}");
                let mut a = session.applied.lock().unwrap_or_else(|e| e.into_inner());
                if !a.contains(&what) {
                    a.push(what);
                }
                Ok(())
            }
            Kind::LogStall { ms, every } => {
                // Not recorded as a probe-blind injector ([`injecting`]): a
                // log stall must cost the workload no latency.
                session.spawn_log_stall(&d, *ms, *every, load_dir(work).join("log-stall"));
                Ok(())
            }
            Kind::Net { netem, flap } => session.place_net(&d, netem, *flap),
            Kind::ClockStep { ms, at } => {
                // Once per node and scenario: the step is cumulative and
                // the file keeps it across the node's restarts (a restarted
                // daemon starts stepped), so a later incarnation must not
                // add it again.
                static ARMED: Mutex<Vec<(String, String, i64, u128)>> = Mutex::new(Vec::new());
                let key = (d.scenario.clone(), node.to_string(), *ms, at.as_millis());
                let mut armed = ARMED.lock().unwrap_or_else(|e| e.into_inner());
                if !armed.contains(&key) {
                    armed.push(key);
                    session.spawn_clock_step(&d, *ms, *at, clock_step_file(work, node));
                }
                Ok(())
            }
            _ => Ok(()),
        };
        if let Err(e) = r {
            // An injector that cannot be applied invalidates the run's
            // load claim: loud, and recorded as a failure of the scenario.
            session.err(format!("load injector {:?} on {node}: {e:#}", i.kind));
        }
    }
}

// ------------------------------------------------------------ session

struct Session {
    scenario: String,
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
    hogs: Option<Child>,
    cgroups: Vec<PathBuf>,
    bursty: Arc<Mutex<Vec<PathBuf>>>,
    probes: Vec<Child>,
    netem: Option<Netem>,
    /// Injectors that could not be applied (the scenario fails on them);
    /// shared with the injector threads.
    errors: Arc<Mutex<Vec<String>>>,
    /// The probe-blind injectors applied ([`injecting`]).
    applied: Arc<Mutex<Vec<String>>>,
    /// The node names daemons were spawned for (a targeted item that
    /// matched none of them is reported).
    spawned: HashSet<String>,
    /// Daemons an injector (not the scenario) holds stopped, mirrored to
    /// [`stopped_file`] for the guard that thaws them if the harness dies.
    stopped: Arc<Mutex<HashSet<u32>>>,
    guard: Option<Child>,
}

impl Session {
    fn err(&self, msg: String) {
        eprintln!("!!! load: {msg}");
        self.errors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(msg);
    }
}

/// Where host-wide load markers live: `/tmp`, whatever `TMPDIR` a run
/// was given (a dm-delay run's points at its slow disk), so every run and
/// `inject.sh cleanup` find every other's.
const MARKER_DIR: &str = "/tmp";

/// What an injector stopped, for the guard (`/tmp/cst-load-stopped-<pid>.pids`).
fn stopped_file() -> PathBuf {
    Path::new(MARKER_DIR).join(format!("cst-load-stopped-{}.pids", std::process::id()))
}

/// Record that an injector stopped (or thawed) `pid`.
fn mark_stopped(set: &Mutex<HashSet<u32>>, pid: u32, stopped: bool) {
    let mut s = set.lock().unwrap_or_else(|e| e.into_inner());
    if stopped {
        s.insert(pid);
    } else {
        s.remove(&pid);
    }
    let list: String = s.iter().map(|p| format!("{p}\n")).collect();
    let tmp = stopped_file().with_extension("tmp");
    if std::fs::write(&tmp, list).is_ok() {
        let _ = std::fs::rename(&tmp, stopped_file());
    }
}

/// A process outside the harness's death signal that thaws whatever the
/// injectors hold stopped once the harness is gone (a stopped daemon
/// ignores the SIGTERM its death signal queues until it is continued).
fn spawn_guard() -> Option<Child> {
    let script = format!(
        "while kill -0 {h} 2>/dev/null; do sleep 1; done; \
         [ -f {f} ] && for p in $(cat {f}); do \
         [ \"$(cat /proc/$p/comm 2>/dev/null)\" = constellation ] && kill -CONT $p 2>/dev/null; \
         done; rm -f {f}",
        h = std::process::id(),
        f = sh_quote(&stopped_file().display().to_string())
    );
    Command::new("sh")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

/// A crashed run's injector-stopped daemons (its guard died too): thaw.
fn sweep_stopped() {
    let Ok(entries) = std::fs::read_dir(MARKER_DIR) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(pid) = name
            .strip_prefix("cst-load-stopped-")
            .and_then(|r| r.strip_suffix(".pids"))
            .and_then(|p| p.parse::<u32>().ok())
        else {
            continue;
        };
        if alive(pid) {
            continue;
        }
        for p in std::fs::read_to_string(e.path())
            .unwrap_or_default()
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
        {
            // Only a stopped daemon: the pid may have been reused.
            if proc_state(p) == Some('T') && is_daemon(p) {
                signal(p, libc::SIGCONT);
                eprintln!("    load: thawed pid {p}, left stopped by a crashed run");
            }
        }
        let _ = std::fs::remove_file(e.path());
    }
}

static SESSION: Mutex<Option<Session>> = Mutex::new(None);

/// The base of this run's cgroups.
fn cgroup_base() -> PathBuf {
    PathBuf::from(format!("/sys/fs/cgroup/cst-load-{}", std::process::id()))
}

/// Start the profile's scenario-wide injectors.
pub fn begin(scenario: &str) {
    let Some(p) = profile() else {
        return;
    };
    let mut session = Session {
        scenario: scenario.to_string(),
        stop: Arc::new(AtomicBool::new(false)),
        threads: Vec::new(),
        hogs: None,
        cgroups: Vec::new(),
        bursty: Arc::new(Mutex::new(Vec::new())),
        probes: Vec::new(),
        netem: None,
        errors: Arc::new(Mutex::new(Vec::new())),
        applied: Arc::new(Mutex::new(Vec::new())),
        spawned: HashSet::new(),
        stopped: Arc::new(Mutex::new(HashSet::new())),
        guard: None,
    };
    if p.items.iter().any(|i| matches!(i.kind, Kind::Pause { .. })) {
        session.guard = spawn_guard();
    }
    for i in &p.items {
        if let Kind::Hogs(h) = &i.kind {
            match start_hogs(h) {
                Ok(c) => session.hogs = Some(c),
                Err(e) => session.err(format!("hogs: {e:#}")),
            }
        }
        if let Kind::Cpu(Cpu::Bursty { hi, lo, period }) = &i.kind {
            let (hi, lo, period) = (*hi, *lo, *period);
            let stop = session.stop.clone();
            let groups = session.bursty.clone();
            session.threads.push(std::thread::spawn(move || {
                let mut high = true;
                while !stop.load(Ordering::Relaxed) {
                    let pct = if high { hi } else { lo };
                    for g in groups.lock().unwrap_or_else(|e| e.into_inner()).iter() {
                        let _ = std::fs::write(g.join("cpu.max"), bursty_max(pct));
                    }
                    high = !high;
                    sleep_unless(&stop, period);
                }
            }));
        }
    }
    *SESSION.lock().unwrap_or_else(|e| e.into_inner()) = Some(session);
}

/// What [`end`] reports.
#[derive(Debug, Default)]
pub struct Ended {
    /// Injectors that could not be applied: the scenario fails on them.
    pub errors: Vec<String>,
    /// Targeted items that matched no daemon of the scenario (applied to
    /// nobody: the results say so).
    pub unapplied: Vec<String>,
}

/// Stop every injector, thaw what they hold stopped, and remove what
/// they made.
pub fn end() -> Ended {
    let Some(mut s) = SESSION.lock().unwrap_or_else(|e| e.into_inner()).take() else {
        return Ended::default();
    };
    s.stop.store(true, Ordering::Relaxed);
    for t in s.threads.drain(..) {
        let _ = t.join();
    }
    // A pauser thaws what it stopped before it ends; anything left (a
    // thread that died) is thawed here, unless the scenario froze it.
    let left: Vec<u32> = s
        .stopped
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .copied()
        .collect();
    for pid in left {
        if !scenario_paused(pid) {
            signal(pid, libc::SIGCONT);
        }
        mark_stopped(&s.stopped, pid, false);
    }
    if let Some(mut g) = s.guard.take() {
        let _ = g.kill();
        let _ = g.wait();
    }
    let _ = std::fs::remove_file(stopped_file());
    if let Some(mut h) = s.hogs.take() {
        // Its own process group: the workers go with it.
        signal_group(h.id(), libc::SIGKILL);
        let _ = h.wait();
    }
    for mut p in s.probes.drain(..) {
        let _ = p.kill();
        let _ = p.wait();
    }
    if let Some(n) = s.netem.take() {
        if let Err(e) = n.teardown() {
            s.err(format!("net teardown: {e:#}"));
        }
    }
    if !s.cgroups.is_empty() {
        if let Err(e) = remove_cgroups(&s.cgroups, true) {
            s.err(format!("cgroup teardown: {e:#}"));
        }
    }
    S3_REWRITE.lock().unwrap_or_else(|e| e.into_inner()).take();
    let unapplied = profile()
        .map(|p| {
            p.items
                .iter()
                .filter_map(|i| match &i.target {
                    Target::Node(n) if !s.spawned.contains(n) => Some(format!(
                        "{:?}@{n}: no daemon named {n} in {}",
                        i.kind, s.scenario
                    )),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    let errors = std::mem::take(&mut *s.errors.lock().unwrap_or_else(|e| e.into_inner()));
    Ended { errors, unapplied }
}

fn signal_group(pgid: u32, sig: libc::c_int) {
    // SAFETY: kill(2) of the process group of a child this harness made
    // its own group's leader.
    unsafe { libc::kill(-(pgid as i32), sig) };
}

fn sleep_unless(stop: &AtomicBool, d: Duration) -> bool {
    let until = Instant::now() + d;
    while Instant::now() < until {
        if stop.load(Ordering::Relaxed) {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50).min(until - Instant::now()));
    }
    !stop.load(Ordering::Relaxed)
}

/// `cpu.max` for `pct` percent of one CPU (period 100 ms).
fn cpu_max(pct: u32) -> String {
    format!("{} 100000", (pct as u64 * 1_000).max(1_000))
}

/// A bursty profile's phase: `100` or more is unthrottled.
fn bursty_max(pct: u32) -> String {
    if pct >= 100 {
        "max 100000".into()
    } else {
        cpu_max(pct)
    }
}

fn start_hogs(h: &Hogs) -> Result<Child> {
    use std::os::unix::process::CommandExt;
    let mut cmd = Command::new("stress-ng");
    // Bounded by the scenario timeout (plus a margin), so a hog leaked
    // past a crash ends by itself.
    let limit = std::env::var("CONSTELLATION_HARNESS_SCENARIO_TIMEOUT_S")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map_or(86_400, |t| t + 300);
    cmd.args(["--timeout", &format!("{limit}s"), "--quiet"]);
    if h.cpu > 0 {
        cmd.args([
            "--cpu",
            &h.cpu.to_string(),
            "--cpu-load",
            &h.cpu_load.to_string(),
        ]);
    }
    if h.vm > 0 {
        cmd.args(["--vm", &h.vm.to_string(), "--vm-bytes", &h.vm_bytes]);
    }
    if h.io > 0 {
        cmd.args(["--io", &h.io.to_string()]);
    }
    if h.hdd > 0 {
        let dir = std::env::temp_dir().join(format!("harness-hogs-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        cmd.args(["--hdd", &h.hdd.to_string(), "--hdd-bytes", &h.hdd_bytes])
            .arg("--temp-path")
            .arg(&dir);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);
    let child = crate::spawn::TiedSpawn::spawn_tied(&mut cmd).context("spawning stress-ng")?;
    eprintln!(
        "    load: stress-ng {} running (pid {})",
        hogs_desc(h),
        child.id()
    );
    Ok(child)
}

fn hogs_desc(h: &Hogs) -> String {
    format!(
        "cpu {}x{}% vm {}x{} io {} hdd {}x{}",
        h.cpu, h.cpu_load, h.vm, h.vm_bytes, h.io, h.hdd, h.hdd_bytes
    )
}

impl Session {
    fn place_cpu(&mut self, d: &Daemon, cpu: &Cpu) -> Result<()> {
        let base = cgroup_base();
        let dir = base.join(format!("{}-{}", d.node, d.pid));
        let max = match cpu {
            Cpu::Quota(p) => cpu_max(*p),
            Cpu::Bursty { hi, .. } => bursty_max(*hi),
        };
        // The probe that sees what the daemon sees, in the same cgroup.
        let exe = std::env::current_exe().context("the harness's own path")?;
        let mut probe = crate::spawn::TiedSpawn::spawn_tied(
            Command::new(exe)
                .args(["host-lag", "--stream"])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null()),
        )
        .context("spawning the cgroup's lag probe")?;
        // SAFETY: no preconditions.
        let uid = unsafe { libc::getuid() };
        let b = sh_quote(&base.display().to_string());
        let g = sh_quote(&dir.display().to_string());
        root_sh(&format!(
            "set -e; mkdir -p {b}; grep -qw cpu {b}/cgroup.subtree_control || echo +cpu > {b}/cgroup.subtree_control; \
             mkdir -p {g}; echo '{max}' > {g}/cpu.max; chown {uid} {g}/cpu.max; \
             echo {pid} > {g}/cgroup.procs; echo {probe} > {g}/cgroup.procs",
            pid = d.pid,
            probe = probe.id(),
        ))?;
        self.cgroups.push(dir.clone());
        if matches!(cpu, Cpu::Bursty { .. }) {
            self.bursty
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(dir.clone());
        }
        if let (Some(out), Some(src)) = (
            probe.stdout.take(),
            crate::hostlag::add_source(&format!("cg:{}", d.node)),
        ) {
            std::thread::spawn(move || {
                use std::io::BufRead;
                for line in std::io::BufReader::new(out).lines() {
                    let Ok(line) = line else { break };
                    if let Some((due, late)) = crate::hostlag::parse_stream_line(&line) {
                        src.push(due, late);
                    }
                }
            });
        }
        self.probes.push(probe);
        eprintln!(
            "    load: {} (pid {}) in {} at cpu.max {}",
            d.node,
            d.pid,
            dir.display(),
            max
        );
        Ok(())
    }

    fn spawn_pauser(
        &mut self,
        d: &Daemon,
        dur: Duration,
        every: Duration,
        step_file: Option<PathBuf>,
    ) {
        let stop = self.stop.clone();
        let stopped = self.stopped.clone();
        let errors = self.errors.clone();
        let d = d.clone();
        self.threads.push(std::thread::spawn(move || {
            let what = if step_file.is_some() {
                "step-pause"
            } else {
                "pause"
            };
            let wait_stopped = |within: Duration| {
                let until = Instant::now() + within;
                while Instant::now() < until && proc_state(d.pid) != Some('T') && alive(d.pid) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                proc_state(d.pid) == Some('T')
            };
            while sleep_unless(&stop, every) && alive(d.pid) {
                if scenario_paused(d.pid) || proc_state(d.pid) == Some('T') {
                    continue;
                }
                let t0 = match &step_file {
                    None => {
                        mark_stopped(&stopped, d.pid, true);
                        signal(d.pid, libc::SIGSTOP);
                        Instant::now()
                    }
                    Some(file) => {
                        let mut marker = file.clone().into_os_string();
                        marker.push(".stopped");
                        let marker = PathBuf::from(marker);
                        let _ = std::fs::remove_file(&marker);
                        if std::fs::write(file, b"stop").is_err() {
                            continue;
                        }
                        // Until the core steps (an idle core may not for
                        // a while), or the session ends.
                        let until = Instant::now() + every;
                        while Instant::now() < until
                            && !stop.load(Ordering::Relaxed)
                            && !marker.exists()
                        {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        // The daemon consumes the file (an atomic
                        // unlink), then writes the marker, then stops:
                        // if our unlink fails, it took it, and stops.
                        if std::fs::remove_file(file).is_ok() && !marker.exists() {
                            continue;
                        }
                        mark_stopped(&stopped, d.pid, true);
                        if !wait_stopped(Duration::from_secs(10)) {
                            let msg = format!(
                                "step-pause: {} (pid {}) consumed the file but never stopped",
                                d.node, d.pid
                            );
                            eprintln!("!!! load: {msg}");
                            errors.lock().unwrap_or_else(|e| e.into_inner()).push(msg);
                            // Still marked stopped: should the stop come
                            // late, `end` (or the guard, if the harness
                            // dies) thaws it.
                            continue;
                        }
                        let _ = std::fs::remove_file(&marker);
                        Instant::now()
                    }
                };
                // Thaw even when the session stops meanwhile.
                std::thread::sleep(dur);
                if !scenario_paused(d.pid) {
                    signal(d.pid, libc::SIGCONT);
                }
                mark_stopped(&stopped, d.pid, false);
                let t1 = Instant::now();
                crate::hostlag::note_injected(&d.node, what, t0, t1);
                eprintln!(
                    "    load: {what} of {} (pid {}) for {:?}",
                    d.node,
                    d.pid,
                    t1 - t0
                );
            }
        }));
    }

    fn spawn_store_stall(&mut self, d: &Daemon, ms: u64, every: Duration, file: PathBuf) {
        let stop = self.stop.clone();
        let d = d.clone();
        self.threads.push(std::thread::spawn(move || {
            while sleep_unless(&stop, every) && alive(d.pid) {
                // Whole, by a rename: a daemon reading a prefix would
                // stall for the wrong time.
                let tmp = file.with_extension("tmp");
                if std::fs::write(&tmp, format!("{ms} once")).is_ok() {
                    let _ = std::fs::rename(&tmp, &file);
                }
                let armed = Instant::now();
                STORE_STALLS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((armed, armed + Duration::from_millis(ms)));
                eprintln!(
                    "    load: store stall of {ms} ms armed on {} (pid {})",
                    d.node, d.pid
                );
            }
            let _ = std::fs::remove_file(&file);
        }));
    }

    /// Stall the daemon's log sink for `ms` every `every`: the file is
    /// written whole (a rename: a daemon reading a prefix would stall for
    /// the wrong time) and names this daemon's pid, so daemons that share
    /// the path stall only their own sink.
    fn spawn_log_stall(&mut self, d: &Daemon, ms: u64, every: Duration, file: PathBuf) {
        let stop = self.stop.clone();
        let d = d.clone();
        self.threads.push(std::thread::spawn(move || {
            while sleep_unless(&stop, every) && alive(d.pid) {
                let tmp = file.with_extension("tmp");
                if std::fs::write(&tmp, format!("{ms} once pid={}", d.pid)).is_ok() {
                    let _ = std::fs::rename(&tmp, &file);
                }
                eprintln!(
                    "    load: log sink stall of {ms} ms armed on {} (pid {})",
                    d.node, d.pid
                );
            }
            let _ = std::fs::remove_file(&file);
        }));
    }

    /// `clock:step:<ms>:<at_ms>`: at `at` after the daemon spawned, step
    /// its protocol wall clock by `ms` (the daemon applies it within
    /// 100 ms and says so in `<file>.applied`). The interval is marked
    /// probe-blind: a step is not latency the host-lag probes can see.
    fn spawn_clock_step(&mut self, d: &Daemon, ms: i64, at: Duration, file: PathBuf) {
        let stop = self.stop.clone();
        let errors = self.errors.clone();
        let applied = self.applied.clone();
        let d = d.clone();
        self.threads.push(std::thread::spawn(move || {
            if !sleep_unless(&stop, at) || !alive(d.pid) {
                return;
            }
            let what = format!("clock:step:{ms}:{}@{}", at.as_millis(), d.node);
            match apply_clock_step(&d.scenario, &d.node, &file, ms)
                .and_then(|()| check_step_counted(&d, ms))
            {
                Ok(()) => {
                    let mut a = applied.lock().unwrap_or_else(|e| e.into_inner());
                    if !a.contains(&what) {
                        a.push(what);
                    }
                }
                Err(e) => {
                    let msg = format!("clock step on {}: {e:#}", d.node);
                    eprintln!("!!! load: {msg}");
                    errors.lock().unwrap_or_else(|e| e.into_inner()).push(msg);
                }
            }
        }));
    }

    fn place_net(&mut self, d: &Daemon, netem: &[String], flap: bool) -> Result<()> {
        if self.netem.is_none() {
            self.netem = Some(Netem::install()?);
        }
        let n = self.netem.as_mut().unwrap();
        let band = n.band_for(d, netem)?;
        let stop = self.stop.clone();
        let errors = self.errors.clone();
        let applied = self.applied.clone();
        let d = d.clone();
        let ports = n.ports.clone();
        let base: Vec<String> = netem.to_vec();
        let what = format!(
            "net:{}{}@{}",
            if flap { "flap:" } else { "" },
            netem.join(":"),
            d.node
        );
        self.threads.push(std::thread::spawn(move || {
            // Its UDP ports appear once the endpoint is bound, and may
            // change on a rebind: rescan while it lives.
            let mut flap_at = Instant::now() + Duration::from_secs(10);
            let mut flapping = false;
            while !stop.load(Ordering::Relaxed) && alive(d.pid) {
                for port in udp_ports(d.pid) {
                    let mut known = ports.lock().unwrap_or_else(|e| e.into_inner());
                    if known.insert(port) {
                        drop(known);
                        if let Err(e) = add_port_filter(band, port) {
                            let msg = format!("netem filter for {} port {port}: {e:#}", d.node);
                            eprintln!("!!! load: {msg}");
                            errors.lock().unwrap_or_else(|e| e.into_inner()).push(msg);
                        } else {
                            eprintln!("    load: netem band {band} on {} UDP port {port}", d.node);
                            let mut a = applied.lock().unwrap_or_else(|e| e.into_inner());
                            if !a.contains(&what) {
                                a.push(what.clone());
                            }
                        }
                    }
                }
                if flap && Instant::now() >= flap_at {
                    let args = if flapping {
                        base.clone()
                    } else {
                        vec!["loss".to_string(), "100%".to_string()]
                    };
                    let _ = change_netem(band, &args);
                    flapping = !flapping;
                    flap_at = Instant::now()
                        + if flapping {
                            Duration::from_secs(2)
                        } else {
                            Duration::from_secs(8)
                        };
                }
                sleep_unless(&stop, Duration::from_millis(500));
            }
        }));
        Ok(())
    }
}

// --------------------------------------------------------------- cgroups

/// Move whatever is left in `dirs` back to the harness's own cgroup and
/// remove them (and the base when it is empty).
fn remove_cgroups(dirs: &[PathBuf], base_too: bool) -> Result<()> {
    let own = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("0::").map(|p| p.trim().to_string()))
        })
        .unwrap_or_else(|| "/".into());
    let own = format!("/sys/fs/cgroup{}", if own == "/" { "" } else { &own });
    let mut script = String::from("set +e; ");
    for d in dirs {
        let g = sh_quote(&d.display().to_string());
        script.push_str(&format!(
            "if [ -d {g} ]; then for p in $(cat {g}/cgroup.procs); do echo $p > {own}/cgroup.procs 2>/dev/null || echo $p > /sys/fs/cgroup/cgroup.procs; done; rmdir {g}; fi; ",
            own = sh_quote(&own)
        ));
    }
    if base_too {
        let b = sh_quote(&cgroup_base().display().to_string());
        script.push_str(&format!("rmdir {b} 2>/dev/null; "));
    }
    script.push_str("true");
    root_sh(&script).map(|_| ())
}

/// A crashed run's cgroups (`cst-load-<pid>` of a dead pid).
fn sweep_cgroups() {
    let Ok(entries) = std::fs::read_dir("/sys/fs/cgroup") else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(pid) = name
            .strip_prefix("cst-load-")
            .and_then(|p| p.parse::<u32>().ok())
        else {
            continue;
        };
        if alive(pid) {
            continue;
        }
        let children: Vec<PathBuf> = std::fs::read_dir(e.path())
            .map(|r| {
                r.flatten()
                    .filter(|c| c.path().is_dir())
                    .map(|c| c.path())
                    .collect()
            })
            .unwrap_or_default();
        let b = sh_quote(&e.path().display().to_string());
        let _ = remove_cgroups(&children, false).and_then(|_| root_sh(&format!("rmdir {b}")));
        eprintln!("    load: swept a crashed run's {}", e.path().display());
    }
}

// --------------------------------------------------------------- netem

struct Netem {
    next_band: u32,
    bands: HashMap<String, u32>,
    ports: Arc<Mutex<HashSet<u16>>>,
}

/// `prio` with 16 bands: the default priomap keeps ordinary traffic in
/// bands 1-3 as before; bands 4-16 carry one netem each.
const PRIO: &str =
    "tc qdisc add dev lo root handle 1: prio bands 16 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1";

impl Netem {
    fn install() -> Result<Netem> {
        let shown = root_sh("tc qdisc show dev lo")?;
        if !shown.contains("noqueue") || shown.contains("prio") {
            bail!(
                "lo already has a root qdisc ({}): another campaign run, or a leftover \
                 (tools/load-campaign/inject.sh cleanup)",
                shown.trim()
            );
        }
        root_sh(PRIO)?;
        let _ = std::fs::write(netem_marker(), std::process::id().to_string());
        Ok(Netem {
            next_band: 4,
            bands: HashMap::new(),
            ports: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// The band of `d` (one per node name and spec), its netem added.
    fn band_for(&mut self, d: &Daemon, netem: &[String]) -> Result<u32> {
        let key = format!("{} {}", d.node, netem.join(" "));
        if let Some(b) = self.bands.get(&key) {
            return Ok(*b);
        }
        anyhow::ensure!(self.next_band <= 16, "more than 13 netem bands");
        let band = self.next_band;
        self.next_band += 1;
        let args = if netem.is_empty() {
            vec!["delay".to_string(), "0ms".to_string()]
        } else {
            netem.to_vec()
        };
        root_sh(&format!(
            "tc qdisc add dev lo parent 1:{band:x} handle {band:x}0: netem {}",
            args.join(" ")
        ))?;
        self.bands.insert(key, band);
        Ok(band)
    }

    fn teardown(self) -> Result<()> {
        let r = root_sh("tc qdisc del dev lo root").map(|_| ());
        let _ = std::fs::remove_file(netem_marker());
        r
    }
}

/// Who installed the `lo` qdisc (`/tmp/cst-load-netem.marker`: the
/// harness's pid, or `inject.sh`'s owner), so a crashed run's is swept
/// and another live run's is not.
fn netem_marker() -> PathBuf {
    Path::new(MARKER_DIR).join("cst-load-netem.marker")
}

/// The `lo` qdisc of a crashed run (its marker names a dead harness).
fn sweep_netem() {
    let Some(pid) = std::fs::read_to_string(netem_marker())
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
    else {
        return;
    };
    if alive(pid) {
        return;
    }
    if root_sh("tc qdisc show dev lo").is_ok_and(|q| q.contains("prio 1: root"))
        && root_sh("tc qdisc del dev lo root").is_ok()
    {
        eprintln!("    load: swept a crashed run's lo qdisc");
    }
    let _ = std::fs::remove_file(netem_marker());
}

fn change_netem(band: u32, args: &[String]) -> Result<()> {
    let args = if args.is_empty() {
        "delay 0ms".to_string()
    } else {
        args.join(" ")
    };
    root_sh(&format!(
        "tc qdisc change dev lo parent 1:{band:x} handle {band:x}0: netem {args}"
    ))
    .map(|_| ())
}

fn add_port_filter(band: u32, port: u16) -> Result<()> {
    let mut script = String::from("set -e; ");
    for dir in ["sport", "dport"] {
        script.push_str(&format!(
            "tc filter add dev lo parent 1: protocol ip prio 1 u32 match ip protocol 17 0xff \
             match ip {dir} {port} 0xffff flowid 1:{band:x}; \
             tc filter add dev lo parent 1: protocol ipv6 prio 2 u32 match ip6 protocol 17 0xff \
             match ip6 {dir} {port} 0xffff flowid 1:{band:x}; "
        ));
    }
    root_sh(&script).map(|_| ())
}

/// The local ports of `pid`'s UDP sockets (`/proc/<pid>/fd` against
/// `/proc/net/udp{,6}`).
pub fn udp_ports(pid: u32) -> Vec<u16> {
    let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else {
        return Vec::new();
    };
    let inodes: HashSet<String> = fds
        .flatten()
        .filter_map(|e| std::fs::read_link(e.path()).ok())
        .filter_map(|l| {
            let s = l.to_string_lossy().into_owned();
            s.strip_prefix("socket:[")
                .and_then(|r| r.strip_suffix(']'))
                .map(str::to_string)
        })
        .collect();
    let mut ports = Vec::new();
    for table in ["udp", "udp6"] {
        let text = std::fs::read_to_string(format!("/proc/{pid}/net/{table}")).unwrap_or_default();
        ports.extend(parse_udp_table(&text, &inodes));
    }
    ports.sort_unstable();
    ports.dedup();
    ports
}

fn parse_udp_table(text: &str, inodes: &HashSet<String>) -> Vec<u16> {
    text.lines()
        .skip(1)
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            let inode = f.get(9)?;
            if !inodes.contains(*inode) {
                return None;
            }
            let port = f.get(1)?.rsplit_once(':')?.1;
            u16::from_str_radix(port, 16).ok().filter(|p| *p != 0)
        })
        .collect()
}

// ------------------------------------------------------------------ S3

/// Toxics this module adds are named so (`crate::toxiproxy` keeps them
/// across a scenario's `heal`).
pub const TOXIC_PREFIX: &str = "load-";

fn s3_items() -> Vec<&'static S3> {
    profile()
        .map(|p| {
            p.items
                .iter()
                .filter_map(|i| match &i.kind {
                    Kind::S3(s) => Some(s),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Whether an `s3:` item is active (the S3 environment then opens the
/// load proxy for its direct clients).
pub fn s3_active() -> bool {
    !s3_items().is_empty()
}

/// Add the profile's S3 toxics to a proxy just created.
pub fn on_s3_proxy(tp: &crate::toxiproxy::Toxiproxy, name: &str) {
    for s in s3_items() {
        let p = tp.proxy(name);
        let r = match s {
            S3::Latency { ms, jitter } => p.load_toxic(
                "latency",
                serde_json::json!({"latency": ms, "jitter": jitter}),
                true,
            ),
            S3::Bandwidth { kbps } => {
                p.load_toxic("bandwidth", serde_json::json!({"rate": kbps}), false)
            }
            S3::Slicer { bytes, delay_us } => p.load_toxic(
                "slicer",
                serde_json::json!({"average_size": bytes, "size_variation": bytes / 2, "delay": delay_us}),
                false,
            ),
            S3::Blackhole { ms, at } => {
                let (api, name, ms, at) = (tp.api().to_string(), name.to_string(), *ms, *at);
                let stop = SESSION
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .map(|s| s.stop.clone());
                let Some(stop) = stop else { continue };
                let h = std::thread::spawn(move || {
                    if !sleep_unless(&stop, at) {
                        return;
                    }
                    let tp = crate::toxiproxy::Toxiproxy::at(&api);
                    let p = tp.proxy(&name);
                    if p
                        .load_toxic("timeout", serde_json::json!({"timeout": 0}), true)
                        .is_ok()
                    {
                        eprintln!("    load: S3 black hole on {name} for {ms} ms");
                        sleep_unless(&stop, Duration::from_millis(ms));
                        let _ = p.remove_load_toxic("timeout", true);
                    }
                });
                if let Some(s) = SESSION.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
                    s.threads.push(h);
                }
                Ok(())
            }
        };
        if r.is_ok() {
            note_applied(format!("{s:?}"));
        }
        if let Err(e) = r {
            let msg = format!("s3 toxic {s:?} on proxy {name}: {e:#}");
            eprintln!("!!! load: {msg}");
            if let Some(s) = SESSION.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
                s.errors.lock().unwrap_or_else(|e| e.into_inner()).push(msg);
            }
        }
    }
}

/// The S3 environment opened its load proxy at `proxied` for clients
/// given `direct`.
pub fn route_direct(direct: &str, proxied: &str) {
    S3_REWRITE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert(direct.to_string(), proxied.to_string());
}

/// The S3 environment of `direct` went away.
pub fn unroute_direct(direct: &str) {
    if let Some(m) = S3_REWRITE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_mut()
    {
        m.remove(direct);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_and_items_parse() {
        for (name, _) in PRESETS {
            parse(name).unwrap_or_else(|e| panic!("{name}: {e:#}"));
        }
        let p = parse("cpu:25@a, hogs:cpu=2:io, s3:latency:500:400, pause:lock-lapse+500:20000@b")
            .unwrap();
        assert_eq!(
            p.items[0],
            Item {
                kind: Kind::Cpu(Cpu::Quota(25)),
                target: Target::Node("a".into())
            }
        );
        match &p.items[1].kind {
            Kind::Hogs(h) => {
                assert_eq!((h.cpu, h.io, h.vm, h.hdd), (2, 4, 0, 0));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            p.items[2].kind,
            Kind::S3(S3::Latency {
                ms: 500,
                jitter: 400
            })
        );
        assert_eq!(
            p.items[3],
            Item {
                kind: Kind::Pause {
                    dur: "lock-lapse+500".into(),
                    every: Duration::from_secs(20),
                    step: false
                },
                target: Target::Node("b".into())
            }
        );
        assert_eq!(parse("baseline").unwrap().items, vec![]);
        // Which node a profile pauses by name (the takeover cells' check).
        let pauses_b = |spec| pauses(&parse(spec).unwrap(), "b");
        assert!(pauses_b("pause:lock-lapse+500:10000@b"));
        assert!(pauses_b("cpu:25, pause:2000:8000@b"));
        assert!(!pauses_b("pause:lock-lapse+500:10000@a"));
        assert!(!pauses_b("pause:lock-lapse+500:10000"), "every node");
        assert!(!pauses_b("step-pause:2000:8000@b"));
        assert!(!pauses_b("baseline"));
        // A preset aimed at one node.
        assert_eq!(
            parse("runtime-hog@b").unwrap().items,
            vec![Item {
                kind: Kind::RuntimeHog("4:200".into()),
                target: Target::Node("b".into())
            }]
        );
        assert_eq!(
            parse("net-lossy").unwrap().items[0].kind,
            Kind::Net {
                netem: ["delay", "30ms", "20ms", "loss", "0.5%", "reorder", "5%"]
                    .map(String::from)
                    .to_vec(),
                flap: false
            }
        );
    }

    /// Plan 44 §6 W3: the log-sink stall item and its preset.
    #[test]
    fn the_log_stall_item_parses_and_is_not_probe_blind() {
        let p = parse("log-stall:3000:4000@b, log-stall:500").unwrap();
        assert_eq!(
            p.items[0],
            Item {
                kind: Kind::LogStall {
                    ms: 3_000,
                    every: Duration::from_secs(4)
                },
                target: Target::Node("b".into())
            }
        );
        assert_eq!(
            p.items[1].kind,
            Kind::LogStall {
                ms: 500,
                every: Duration::from_secs(15)
            }
        );
        assert_eq!(
            parse("log-stall12@b").unwrap().items,
            parse("log-stall:12000:20000@b").unwrap().items
        );
        assert!(parse("log-stall:fast").is_err());
    }

    /// Expectation: `clock:step:<ms>:<at_ms>[@node]` parses with its sign
    /// and its node, `@` is only the node separator, and a step of 0 or
    /// without a time is refused.
    #[test]
    fn clock_steps_parse() {
        let p =
            parse("clock:step:+3000:30000@a,clock:step:-3000:60000@b,clock:step:500:0").unwrap();
        assert_eq!(
            p.items,
            vec![
                Item {
                    kind: Kind::ClockStep {
                        ms: 3_000,
                        at: Duration::from_secs(30)
                    },
                    target: Target::Node("a".into())
                },
                Item {
                    kind: Kind::ClockStep {
                        ms: -3_000,
                        at: Duration::from_secs(60)
                    },
                    target: Target::Node("b".into())
                },
                Item {
                    kind: Kind::ClockStep {
                        ms: 500,
                        at: Duration::ZERO
                    },
                    target: Target::All
                },
            ]
        );
        // The constant offset's cap (±500 ms) does not apply to a step.
        assert!(parse("clock:step:-8000:1000@a").is_ok());
        for bad in [
            "clock:step:0:1000",
            "clock:step",
            "clock:step:x:1",
            "clock:step:5:y",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
    }

    /// Expectation: `clock:rate:<±ppm>[:unbudgeted][@node]` parses with its
    /// sign, its flag and its node; a zero rate, one from 500 000 ppm and
    /// an unknown flag are refused.
    #[test]
    fn clock_rates_parse() {
        let p = parse("clock:rate:+800@a,clock:rate:-50000:unbudgeted@b,clock:rate:1000").unwrap();
        assert_eq!(
            p.items,
            vec![
                Item {
                    kind: Kind::ClockRate {
                        ppm: 800,
                        budgeted: true
                    },
                    target: Target::Node("a".into())
                },
                Item {
                    kind: Kind::ClockRate {
                        ppm: -50_000,
                        budgeted: false
                    },
                    target: Target::Node("b".into())
                },
                Item {
                    kind: Kind::ClockRate {
                        ppm: 1_000,
                        budgeted: true
                    },
                    target: Target::All
                },
            ]
        );
        assert!(parse("clock:rate:499999").is_ok());
        for bad in [
            "clock:rate",
            "clock:rate:0",
            "clock:rate:x",
            "clock:rate:500000",
            "clock:rate:-500000",
            "clock:rate:100:budgeted",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn bad_items_are_refused() {
        for bad in [
            "cpu",
            "cpu:0",
            "warp:9",
            "clock:900",
            "clock:spread:600",
            "pause:never",
            "runtime-hog:4",
            "s3:wobble",
            "net",
            "hogs:gpu",
            "log-stall",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn cpu_max_values() {
        assert_eq!(cpu_max(25), "25000 100000");
        assert_eq!(cpu_max(2), "2000 100000");
        assert_eq!(cpu_max(100), "100000 100000");
        assert_eq!(bursty_max(100), "max 100000");
        assert_eq!(bursty_max(2), "2000 100000");
        assert_eq!(cpu_max(250), "250000 100000");
    }

    #[test]
    fn udp_tables_parse() {
        let text = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n\
          1: 0100007F:1F90 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 4242 2 0000000000000000 0\n\
          2: 00000000:0035 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 999 2 0000000000000000 0\n";
        let inodes: HashSet<String> = ["4242".to_string()].into();
        assert_eq!(parse_udp_table(text, &inodes), vec![8080]);
        // This process's own UDP socket is found.
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = s.local_addr().unwrap().port();
        assert!(udp_ports(std::process::id()).contains(&port));
    }
}
