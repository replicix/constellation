//! Directory hash-range split benchmark (`dirsplit-bench`, run by name
//! only): does splitting one hot shared directory into name-hash ranges
//! (plan 30 §M12, "GIGA+") pay off, and under which network topology?
//!
//! One root (`r`, never a writer) and `N` writer nodes (`w0`..), each
//! placed on a *site*. A site has its own S3 latency (a toxiproxy route
//! per node, latency toxics both ways, so one request/response costs the
//! site's latency) and a one-way P2P delay to every other site (`tc
//! netem` on `lo`, one band per delay, a `u32` filter per ordered pair of
//! the daemons' UDP ports, installed as each daemon mounts so every P2P
//! link is born delayed). Every phase runs one workload on a fresh
//! directory under one configuration:
//!
//! - `a`: hash-range splits off (`CONSTELLATION_DELEGATION_SPLIT=0`; the
//!   placement still delegates subtrees);
//! - `b`: the default automatic placement (`SPLIT=20`); the phase reports
//!   whether a split fired;
//! - `c`: a hand split, one range per writer, range `i` to writer `i`;
//! - `d`: the directory delegated whole to `w0`.
//!
//! `a`, `c` and `d` share a cluster (mounted with `SPLIT=0`); `b` needs
//! its own (the knob is read at mount). Workloads: `uniform` (creates of
//! random names), `mix` (create, and unlink the thread's 16th-newest
//! file), `rename` (rename a thread's own file to a random new name) and
//! `own` (creates of names hashing into the writer's own range of `N`:
//! the synthetic best case for a split). Every op is a FUSE call on the
//! writer's own mount; files are empty, so no data reaches S3.
//!
//! Settings come from the environment (`DIRSPLIT_*`, see [`Spec`]); one
//! JSON object per phase is appended to `DIRSPLIT_OUT`. Needs the process
//! S3 backend (`--s3-backend process`) and `sudo -n tc`. The scripts
//! and the report are in `bench/dirsplit/`.

use super::m11::{
    deleg_of, delegate, delegate_range, n, node_id, undelegate, unmount_all, wait_installed,
    wait_installed_range,
};
use super::{eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use crate::s3env::BUCKET;
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const NAME: &str = "dirsplit-bench";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    /// Every node on site 0.
    Same,
    /// The root and the first half of the writers on site 0, the rest on
    /// site 1.
    Half,
    /// The root on site 0, every writer on site 1.
    RootFar,
    /// Every node on a site of its own (the root on site 0).
    Each,
}

#[derive(Clone, Debug)]
struct Spec {
    label: String,
    rep: u32,
    writers: usize,
    layout: Layout,
    /// S3 latency (one request/response) on site 0, and on every other.
    s3_near_ms: u64,
    s3_far_ms: u64,
    /// P2P round trip within a site and between two sites.
    rtt_local_ms: f64,
    rtt_remote_ms: f64,
    workloads: Vec<String>,
    configs: Vec<char>,
    /// Rotate the configuration order by this much (alternation across
    /// repetitions).
    rotate: usize,
    threads: usize,
    warm_s: u64,
    /// The warm-up of a `b` phase (the placement window must see the
    /// load before it can split).
    bwarm_s: u64,
    measure_s: u64,
    out: std::path::PathBuf,
    extra_env: Vec<(String, String)>,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse_num<T: std::str::FromStr>(key: &str, default: &str) -> Result<T> {
    let v = env_or(key, default);
    v.trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("{key}={v:?} is not a number"))
}

impl Spec {
    fn from_env() -> Result<Spec> {
        let layout = match env_or("DIRSPLIT_LAYOUT", "same").as_str() {
            "same" => Layout::Same,
            "half" => Layout::Half,
            "rootfar" => Layout::RootFar,
            "each" => Layout::Each,
            o => bail!("DIRSPLIT_LAYOUT={o:?}: same, half, rootfar or each"),
        };
        let writers: usize = parse_num("DIRSPLIT_WRITERS", "4")?;
        anyhow::ensure!(
            [2, 4, 8, 16].contains(&writers),
            "DIRSPLIT_WRITERS must be 2, 4, 8 or 16 (one range each)"
        );
        let configs: Vec<char> = env_or("DIRSPLIT_CONFIGS", "a,c,d")
            .split(',')
            .filter_map(|s| s.trim().chars().next())
            .collect();
        anyhow::ensure!(
            configs.iter().all(|c| "abcd".contains(*c)),
            "DIRSPLIT_CONFIGS: a, b, c, d"
        );
        anyhow::ensure!(
            !configs.contains(&'b') || configs.len() == 1,
            "DIRSPLIT_CONFIGS: b needs a cluster of its own (SPLIT is read at mount)"
        );
        let workloads: Vec<String> = env_or("DIRSPLIT_WORKLOADS", "uniform,mix,rename,own")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        for w in &workloads {
            anyhow::ensure!(
                ["uniform", "mix", "rename", "own"].contains(&w.as_str()),
                "unknown workload {w:?}"
            );
        }
        let extra_env = env_or("DIRSPLIT_ENV", "")
            .split(',')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            .collect();
        Ok(Spec {
            label: env_or("DIRSPLIT_LABEL", "unlabelled"),
            rep: parse_num("DIRSPLIT_REP", "0")?,
            writers,
            layout,
            s3_near_ms: parse_num("DIRSPLIT_S3_NEAR_MS", "20")?,
            s3_far_ms: parse_num("DIRSPLIT_S3_FAR_MS", "80")?,
            rtt_local_ms: parse_num("DIRSPLIT_RTT_LOCAL_MS", "0.5")?,
            rtt_remote_ms: parse_num("DIRSPLIT_RTT_REMOTE_MS", "60")?,
            workloads,
            configs,
            rotate: parse_num("DIRSPLIT_ROTATE", "0")?,
            threads: parse_num("DIRSPLIT_THREADS", "4")?,
            warm_s: parse_num("DIRSPLIT_WARM_S", "5")?,
            bwarm_s: parse_num("DIRSPLIT_BWARM_S", "20")?,
            measure_s: parse_num("DIRSPLIT_MEASURE_S", "15")?,
            out: env_or("DIRSPLIT_OUT", "dirsplit.jsonl").into(),
            extra_env,
        })
    }

    /// The site of node `i` (0 the root, `1..=N` the writers).
    fn site(&self, i: usize) -> usize {
        match self.layout {
            Layout::Same => 0,
            Layout::Half => usize::from(i > self.writers / 2),
            Layout::RootFar => usize::from(i > 0),
            Layout::Each => i,
        }
    }

    fn s3_ms(&self, i: usize) -> u64 {
        if self.site(i) == 0 {
            self.s3_near_ms
        } else {
            self.s3_far_ms
        }
    }

    /// The one-way delay between nodes `i` and `j`, in microseconds.
    fn one_way_us(&self, i: usize, j: usize) -> u64 {
        let rtt = if self.site(i) == self.site(j) {
            self.rtt_local_ms
        } else {
            self.rtt_remote_ms
        };
        (rtt * 500.0).round() as u64
    }

    fn bits(&self) -> u8 {
        self.writers.trailing_zeros() as u8
    }
}

// ------------------------------------------------------------ netem

/// The `lo` qdisc this run installs: one netem band per distinct delay,
/// a filter per ordered pair of daemon UDP ports.
struct PairNetem {
    bands: BTreeMap<u64, u32>,
    /// Each daemon's UDP ports, by node index.
    ports: BTreeMap<usize, Vec<u16>>,
    filters: usize,
}

const NETEM_MARKER: &str = "/tmp/cst-dirsplit-netem.marker";

impl PairNetem {
    fn install() -> Result<PairNetem> {
        let shown = crate::load::root_sh("tc qdisc show dev lo")?;
        if shown.contains("prio") || shown.contains("netem") {
            // A crashed run of this bench leaves its marker; anything else
            // is someone else's.
            let ours = std::fs::read_to_string(NETEM_MARKER)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .is_some_and(|pid| !crate::load::alive(pid));
            anyhow::ensure!(ours, "lo already has a qdisc: {}", shown.trim());
            crate::load::root_sh("tc qdisc del dev lo root")?;
        }
        crate::load::root_sh(
            "tc qdisc add dev lo root handle 1: prio bands 16 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1",
        )?;
        std::fs::write(NETEM_MARKER, std::process::id().to_string())?;
        Ok(PairNetem {
            bands: BTreeMap::new(),
            ports: BTreeMap::new(),
            filters: 0,
        })
    }

    fn band(&mut self, us: u64) -> Result<u32> {
        if let Some(b) = self.bands.get(&us) {
            return Ok(*b);
        }
        let band = 4 + self.bands.len() as u32;
        anyhow::ensure!(band <= 16, "too many distinct delays");
        crate::load::root_sh(&format!(
            "tc qdisc add dev lo parent 1:{band:x} handle {band:x}0: netem delay {us}us limit 200000"
        ))?;
        self.bands.insert(us, band);
        Ok(band)
    }

    /// Node `i` (pid `pid`) is up: delay the traffic of its UDP ports
    /// not placed yet with every port of every other node placed. Called
    /// again later it places the ports the daemon opened since (one that
    /// closed keeps its filters, which match nothing).
    fn place(&mut self, spec: &Spec, i: usize, pid: u32) -> Result<usize> {
        let now = crate::load::udp_ports(pid);
        anyhow::ensure!(!now.is_empty(), "node {i} (pid {pid}) has no UDP port yet");
        let known = self.ports.get(&i).cloned().unwrap_or_default();
        let fresh: Vec<u16> = now.into_iter().filter(|p| !known.contains(p)).collect();
        let mut script = String::from("set -e; ");
        let others: Vec<(usize, Vec<u16>)> = self
            .ports
            .iter()
            .filter(|(k, _)| **k != i)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        for (j, theirs) in &others {
            let us = spec.one_way_us(i, *j);
            if us == 0 {
                continue;
            }
            let band = self.band(us)?;
            for p in &fresh {
                for q in theirs {
                    for (s, d) in [(p, q), (q, p)] {
                        script.push_str(&format!(
                            "tc filter add dev lo parent 1: protocol ip prio 1 u32 \
                             match ip protocol 17 0xff match ip sport {s} 0xffff \
                             match ip dport {d} 0xffff flowid 1:{band:x}; \
                             tc filter add dev lo parent 1: protocol ipv6 prio 2 u32 \
                             match ip6 protocol 17 0xff match ip6 sport {s} 0xffff \
                             match ip6 dport {d} 0xffff flowid 1:{band:x}; "
                        ));
                        self.filters += 2;
                    }
                }
            }
        }
        if script.len() > "set -e; ".len() {
            crate::load::root_sh(&script)?;
        }
        let entry = self.ports.entry(i).or_default();
        entry.extend(fresh.iter().copied());
        Ok(fresh.len())
    }

    fn teardown(&self) {
        let _ = crate::load::root_sh("tc qdisc del dev lo root");
        let _ = std::fs::remove_file(NETEM_MARKER);
    }
}

impl Drop for PairNetem {
    fn drop(&mut self) {
        self.teardown();
    }
}

// ------------------------------------------------------------ cluster

struct Cluster {
    _env: crate::s3env::S3Env,
    _root: tempfile::TempDir,
    clients: Vec<Client>,
    proxies: Vec<CountingProxy>,
    netem: PairNetem,
}

fn build_cluster(spec: &Spec, split_pct: &str) -> Result<Cluster> {
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut names = vec!["r".to_string()];
    names.extend((0..spec.writers).map(|i| format!("w{i}")));
    let mut proxies = Vec::new();
    let mut clients = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let s3 = spec.s3_ms(i);
        let upstream = env.site_proxy(&format!("dirsplit-{name}"), s3 / 2, s3 / 20)?;
        let p = CountingProxy::start(&upstream)?;
        let mut c = Client::new(root.path(), name, &p.endpoint(), &backend)?
            .with_own_node_key()
            // The root stays where the topology put it: no lease
            // placement, no handoff on forwarded demand, no idle release.
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
            .with_env("CONSTELLATION_LEASE_WANTED_GRACE_MS", "600000")
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
            .with_env("CONSTELLATION_DELEGATION_SPLIT", split_pct);
        for (k, v) in &spec.extra_env {
            c = c.with_env(k, v);
        }
        proxies.push(p);
        clients.push(c);
    }
    let mut netem = PairNetem::install()?;
    clients[0].fs_create()?;
    for (i, c) in clients.iter_mut().enumerate() {
        c.mount()?;
        let pid = c.pid().context("a mounted daemon has no pid")?;
        // Its P2P endpoint binds at start; give it a moment if not yet.
        let mut placed = netem.place(spec, i, pid);
        for _ in 0..50 {
            if placed.is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
            placed = netem.place(spec, i, pid);
        }
        placed?;
    }
    let refs: Vec<&Client> = clients.iter().collect();
    wait_for_p2p(&refs)?;
    super::m11::wait_for_connected_peers(&refs)?;
    // A port a daemon opened after its placement is placed now (and
    // again before every phase: [`refresh_netem`]).
    let late = refresh_netem(spec, &clients, &mut netem)?;
    if late > 0 {
        eprintln!("    {NAME}: {late} UDP port(s) opened after the mount, placed");
    }
    std::fs::write(clients[0].mnt.join("f"), b"initial")?;
    eventually("r holds the lease", Duration::from_secs(30), || {
        let lease = lease_of(&clients[0])?;
        anyhow::ensure!(lease["held"] == true, "r does not hold: {lease}");
        Ok(())
    })?;
    for c in &clients[1..] {
        eventually("f visible everywhere", Duration::from_secs(60), || {
            anyhow::ensure!(c.mnt.join("f").exists(), "f not visible on {}", c.name);
            Ok(())
        })?;
    }
    eprintln!(
        "    {NAME}: {} nodes up; netem bands {:?} (one-way us -> band), {} filters, ports {:?}",
        clients.len(),
        netem.bands,
        netem.filters,
        netem.ports
    );
    Ok(Cluster {
        _env: env,
        _root: root,
        clients,
        proxies,
        netem,
    })
}

/// Place every daemon's UDP ports opened since its last placement; how
/// many there were.
fn refresh_netem(spec: &Spec, clients: &[Client], netem: &mut PairNetem) -> Result<usize> {
    let mut late = 0;
    for (i, c) in clients.iter().enumerate() {
        late += netem.place(spec, i, c.pid().unwrap_or(0))?;
    }
    Ok(late)
}

// ------------------------------------------------------------ workload

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OpKind {
    Create,
    Unlink,
    Rename,
}

struct OpRec {
    start: Instant,
    lat_us: u32,
    kind: OpKind,
    err: Option<String>,
}

struct ThreadOut {
    ops: Vec<OpRec>,
    /// The names this thread leaves in the directory.
    live: Vec<String>,
    /// Names whose op failed: present or not.
    doubtful: Vec<String>,
}

struct Names {
    rng: Rng,
    /// `Some(idx)`: only names hashing into range `idx` of `bits`.
    own: Option<(u8, u32)>,
    prefix: String,
}

impl Names {
    fn next(&mut self) -> String {
        loop {
            let name = format!("{}{:016x}", self.prefix, self.rng.next());
            match self.own {
                None => return name,
                Some((bits, idx)) => {
                    if constellation_meta::delegation::Range::of(bits, &name).idx == idx {
                        return name;
                    }
                }
            }
        }
    }
}

fn errname(e: &std::io::Error) -> String {
    e.raw_os_error()
        .map(|c| format!("errno{c}"))
        .unwrap_or_else(|| format!("{:?}", e.kind()))
}

fn timed<T>(f: impl FnOnce() -> std::io::Result<T>) -> (u32, Option<String>) {
    let t = Instant::now();
    let r = f();
    let us = t.elapsed().as_micros().min(u128::from(u32::MAX)) as u32;
    (us, r.err().map(|e| errname(&e)))
}

fn create(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map(drop)
}

/// One writer thread: runs `workload` on `dir` until `stop`.
fn run_thread(
    dir: std::path::PathBuf,
    workload: String,
    mut names: Names,
    stop: Arc<AtomicBool>,
) -> ThreadOut {
    let mut out = ThreadOut {
        ops: Vec::new(),
        live: Vec::new(),
        doubtful: Vec::new(),
    };
    let mut pool: std::collections::VecDeque<String> = Default::default();
    if workload == "rename" {
        for _ in 0..16 {
            let n = names.next();
            match create(&dir.join(&n)) {
                Ok(()) => pool.push_back(n),
                Err(_) => out.doubtful.push(n),
            }
        }
    }
    let mut i = 0u64;
    while !stop.load(Ordering::Relaxed) {
        i += 1;
        match workload.as_str() {
            "uniform" | "own" => {
                let n = names.next();
                let start = Instant::now();
                let (lat_us, err) = timed(|| create(&dir.join(&n)));
                if err.is_some() {
                    out.doubtful.push(n);
                } else {
                    out.live.push(n);
                }
                out.ops.push(OpRec {
                    start,
                    lat_us,
                    kind: OpKind::Create,
                    err,
                });
            }
            "mix" => {
                let n = names.next();
                let start = Instant::now();
                let (lat_us, err) = timed(|| create(&dir.join(&n)));
                if err.is_some() {
                    out.doubtful.push(n);
                } else {
                    pool.push_back(n);
                }
                out.ops.push(OpRec {
                    start,
                    lat_us,
                    kind: OpKind::Create,
                    err,
                });
                if pool.len() > 16 {
                    let old = pool.pop_front().expect("non-empty");
                    let start = Instant::now();
                    let (lat_us, err) = timed(|| std::fs::remove_file(dir.join(&old)));
                    if err.is_some() {
                        out.doubtful.push(old);
                    }
                    out.ops.push(OpRec {
                        start,
                        lat_us,
                        kind: OpKind::Unlink,
                        err,
                    });
                }
            }
            "rename" => {
                let Some(old) = pool.pop_front() else {
                    // Every file of the pool went doubtful.
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                };
                let new = names.next();
                let start = Instant::now();
                let (lat_us, err) = timed(|| std::fs::rename(dir.join(&old), dir.join(&new)));
                if err.is_some() {
                    out.doubtful.push(old);
                    out.doubtful.push(new);
                } else {
                    pool.push_back(new);
                }
                out.ops.push(OpRec {
                    start,
                    lat_us,
                    kind: OpKind::Rename,
                    err,
                });
            }
            _ => unreachable!(),
        }
        let _ = i;
    }
    out.live.extend(pool);
    out
}

fn pct(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    f64::from(sorted[idx]) / 1000.0
}

fn lat_summary(mut v: Vec<u32>) -> Value {
    v.sort_unstable();
    json!({
        "n": v.len(),
        "p50_ms": pct(&v, 0.50),
        "p90_ms": pct(&v, 0.90),
        "p99_ms": pct(&v, 0.99),
        "max_ms": v.last().map(|x| f64::from(*x) / 1000.0),
    })
}

/// utime + stime of `pid`, in seconds.
fn cpu_s(pid: u32) -> f64 {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return f64::NAN;
    };
    let Some(rest) = stat.rsplit_once(')').map(|(_, r)| r) else {
        return f64::NAN;
    };
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After the comm: state is field 3, utime 14, stime 15.
    let utime: f64 = f.get(11).and_then(|s| s.parse().ok()).unwrap_or(f64::NAN);
    let stime: f64 = f.get(12).and_then(|s| s.parse().ok()).unwrap_or(f64::NAN);
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    (utime + stime) / hz
}

/// The ranges (or the whole entry) of `path` in `c`'s delegation table:
/// `[(range, node)]`, `""` for a whole delegation.
fn table_of(c: &Client, path: &str) -> Vec<(String, u64)> {
    deleg_of(c)
        .ok()
        .and_then(|d| d["table"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter(|e| e["path"] == path)
        .map(|e| (e["range"].as_str().unwrap_or("").to_string(), n(e, "node")))
        .collect()
}

fn is_split(table: &[(String, u64)]) -> bool {
    table.iter().any(|(r, _)| !r.is_empty())
}

const DELEG_COUNTERS: &[&str] = &[
    "executed",
    "fast_path_executed",
    "forwarded_to_delegate",
    "cross_subtree",
    "recalls_sent",
    "recalls_drained",
    "recalls_expired",
    "redelegated",
    "deps_waits",
    "streamed_txs",
    "appended_txs",
    "place_splits",
    "place_range_recalls",
    "place_delegated",
    "place_recalled",
    "lapsed",
    "grants_lost",
];

fn deleg_counters(c: &Client) -> BTreeMap<String, u64> {
    let d = deleg_of(c).unwrap_or_default();
    DELEG_COUNTERS
        .iter()
        .map(|k| (k.to_string(), n(&d, k)))
        .collect()
}

fn counters_delta(a: &BTreeMap<String, u64>, b: &BTreeMap<String, u64>) -> Value {
    let m: serde_json::Map<String, Value> = b
        .iter()
        .map(|(k, v)| (k.clone(), json!(v.saturating_sub(*a.get(k).unwrap_or(&0)))))
        .filter(|(_, v)| v.as_u64() != Some(0))
        .collect();
    Value::Object(m)
}

/// The P2P RTTs (ms, as `status` rounds them) and connectivity `c` sees.
fn peer_view(c: &Client) -> Value {
    let s = c.control_status().unwrap_or_default();
    let peers: Vec<Value> = super::node_peers(&s["p2p"])
        .map(|p| {
            json!({
                "node": p["node_id"],
                "rtt_ms": p["rtt_ms"],
                "connected": p["connected"],
            })
        })
        .collect();
    json!({
        "peers": peers,
        "ack_backups": s["ack"]["backups"],
        "deleg_backups": s["delegation"]["backups"],
    })
}

// ------------------------------------------------------------ phases

fn run_phase(
    spec: &Spec,
    cl: &Cluster,
    ids: &[u64],
    seq: usize,
    workload: &str,
    config: char,
) -> Result<Value> {
    let clients = &cl.clients;
    let root = &clients[0];
    let writers = &clients[1..];
    let dir = format!("p{seq}-{workload}-{config}");
    let path = format!("/{dir}");
    std::fs::create_dir(root.mnt.join(&dir))?;
    for w in writers {
        eventually(&format!("{dir} visible"), Duration::from_secs(60), || {
            anyhow::ensure!(w.mnt.join(&dir).is_dir(), "not yet on {}", w.name);
            Ok(())
        })?;
    }
    let count = spec.writers;
    match config {
        'c' => {
            for (i, w) in writers.iter().enumerate() {
                let range = format!("{i}/{count}");
                delegate_range(root, &path, ids[i + 1], &range)?;
                wait_installed_range(w, &path, &range, Duration::from_secs(60))?;
            }
        }
        'd' => {
            delegate(root, &path, ids[1])?;
            wait_installed(&writers[0], &path, Duration::from_secs(60))?;
        }
        _ => {}
    }
    let stop = Arc::new(AtomicBool::new(false));
    let t_launch = Instant::now();
    let mut handles = Vec::new();
    for (wi, w) in writers.iter().enumerate() {
        for t in 0..spec.threads {
            let names = Names {
                rng: Rng((ts() as u64)
                    ^ ((seq as u64) << 40)
                    ^ ((wi as u64) << 20)
                    ^ (t as u64 + 1)
                    | 1),
                own: (workload == "own").then(|| (spec.bits(), wi as u32)),
                prefix: format!("{}t{t}-", w.name),
            };
            let (d, wl, st) = (w.mnt.join(&dir), workload.to_string(), stop.clone());
            handles.push((wi, std::thread::spawn(move || run_thread(d, wl, names, st))));
        }
    }
    // Warm-up; a `b` phase watches for its split meanwhile.
    let warm = Duration::from_secs(if config == 'b' {
        spec.bwarm_s
    } else {
        spec.warm_s
    });
    let mut split_after: Option<f64> = None;
    while t_launch.elapsed() < warm {
        if config == 'b' && split_after.is_none() && is_split(&table_of(root, &path)) {
            split_after = Some(t_launch.elapsed().as_secs_f64());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    // Measurement.
    for p in &cl.proxies {
        p.reset();
    }
    let cpu0: Vec<f64> = clients
        .iter()
        .map(|c| cpu_s(c.pid().unwrap_or(0)))
        .collect();
    let deleg0: Vec<_> = clients.iter().map(deleg_counters).collect();
    let table_start = table_of(root, &path);
    let views = clients.iter().map(peer_view).collect::<Vec<_>>();
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(spec.measure_s) {
        if config == 'b' && split_after.is_none() && is_split(&table_of(root, &path)) {
            split_after = Some(t_launch.elapsed().as_secs_f64());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let t_end = Instant::now();
    let wall = (t_end - t0).as_secs_f64();
    let cpu1: Vec<f64> = clients
        .iter()
        .map(|c| cpu_s(c.pid().unwrap_or(0)))
        .collect();
    let s3: Vec<Value> = cl
        .proxies
        .iter()
        .zip(clients)
        .map(|(p, c)| {
            let t = crate::reqlog::tally(&p.requests());
            json!({"node": c.name, "put": t.put, "get": t.get, "list": t.list, "head": t.head,
                   "delete": t.delete, "total": t.total()})
        })
        .collect();
    let deleg1: Vec<_> = clients.iter().map(deleg_counters).collect();
    let table_end = table_of(root, &path);
    stop.store(true, Ordering::Relaxed);
    // Join, bounded: a thread stuck in a FUSE call past this fails the
    // phase (and the cluster: the stuck call keeps the mount busy).
    let join_by = Instant::now() + Duration::from_secs(240);
    let mut outs: Vec<(usize, ThreadOut)> = Vec::new();
    for (wi, h) in handles {
        while !h.is_finished() {
            anyhow::ensure!(
                Instant::now() < join_by,
                "{dir}: a writer thread of w{wi} is stuck in a file system call"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        outs.push((
            wi,
            h.join()
                .map_err(|_| anyhow::anyhow!("writer thread panicked"))?,
        ));
    }
    let drain_s = (Instant::now() - t_end).as_secs_f64();
    // Results over the ops that started inside the window.
    let mut per_node = vec![0u64; spec.writers];
    let mut by_kind: BTreeMap<OpKind, Vec<u32>> = BTreeMap::new();
    let mut all = Vec::new();
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    let mut errors_total = 0u64;
    let mut expected: HashSet<String> = HashSet::new();
    let mut doubtful: HashSet<String> = HashSet::new();
    for (wi, o) in &outs {
        for op in &o.ops {
            if op.start < t0 || op.start >= t_end {
                // An error in the warm-up still counts.
                if let Some(e) = &op.err {
                    errors_total += 1;
                    *errors.entry(format!("{:?}:{e}", op.kind)).or_default() += 1;
                }
                continue;
            }
            match &op.err {
                None => {
                    per_node[*wi] += 1;
                    all.push(op.lat_us);
                    by_kind.entry(op.kind).or_default().push(op.lat_us);
                }
                Some(e) => {
                    errors_total += 1;
                    *errors.entry(format!("{:?}:{e}", op.kind)).or_default() += 1;
                }
            }
        }
        expected.extend(o.live.iter().cloned());
        doubtful.extend(o.doubtful.iter().cloned());
    }
    // Convergence: the root and the last writer list exactly the names
    // the writers left (a doubtful name may or may not be there).
    let t_conv = Instant::now();
    let mut converged = Vec::new();
    for c in [&clients[0], clients.last().expect("writers")] {
        let r = eventually(
            &format!("{} lists {dir}", c.name),
            Duration::from_secs(120),
            || {
                let got: HashSet<String> = std::fs::read_dir(c.mnt.join(&dir))?
                    .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
                    .collect();
                let missing = expected.iter().filter(|n| !got.contains(*n)).count();
                let extra = got
                    .iter()
                    .filter(|n| !expected.contains(*n) && !doubtful.contains(*n))
                    .count();
                anyhow::ensure!(
                    missing == 0 && extra == 0,
                    "{} lists {} names: {missing} missing, {extra} unexpected",
                    c.name,
                    got.len()
                );
                Ok(())
            },
        );
        converged.push(json!({
            "node": c.name,
            "ok": r.is_ok(),
            "after_s": t_conv.elapsed().as_secs_f64(),
            "err": r.err().map(|e| format!("{e:#}")),
        }));
    }
    // Undo the configuration, and whatever the placement made of the
    // directory, so the next phase starts clean (a node holding a whole
    // delegation can be given no other).
    if !table_of(root, &path).is_empty() {
        undelegate(root, &path)?;
        eventually(&format!("{path} recalled"), Duration::from_secs(60), || {
            anyhow::ensure!(table_of(root, &path).is_empty(), "still delegated");
            Ok(())
        })?;
    }
    let total: u64 = per_node.iter().sum();
    let cpu: Vec<Value> = clients
        .iter()
        .zip(cpu0.iter().zip(&cpu1))
        .map(|(c, (a, b))| json!({"node": c.name, "pct": 100.0 * (b - a) / wall}))
        .collect();
    let deleg: Vec<Value> = clients
        .iter()
        .zip(deleg0.iter().zip(&deleg1))
        .map(|(c, (a, b))| json!({"node": c.name, "delta": counters_delta(a, b)}))
        .collect();
    let kinds: serde_json::Map<String, Value> = by_kind
        .into_iter()
        .map(|(k, v)| (format!("{k:?}").to_lowercase(), lat_summary(v)))
        .collect();
    let lat = lat_summary(all);
    let r = json!({
        "label": spec.label,
        "rep": spec.rep,
        "writers": spec.writers,
        "layout": format!("{:?}", spec.layout).to_lowercase(),
        "s3_near_ms": spec.s3_near_ms,
        "s3_far_ms": spec.s3_far_ms,
        "rtt_local_ms": spec.rtt_local_ms,
        "rtt_remote_ms": spec.rtt_remote_ms,
        "sites": (0..=spec.writers).map(|i| spec.site(i)).collect::<Vec<_>>(),
        "threads": spec.threads,
        "workload": workload,
        "synthetic": workload == "own",
        "config": config.to_string(),
        "seq": seq,
        "measure_s": wall,
        "drain_s": drain_s,
        "ops": total,
        "ops_per_s": total as f64 / wall,
        "per_node_ops_per_s": per_node.iter().map(|x| *x as f64 / wall).collect::<Vec<_>>(),
        "latency": lat,
        "by_kind": kinds,
        "errors_total": errors_total,
        "errors": errors,
        "cpu": cpu,
        "root_cpu_pct": 100.0 * (cpu1[0] - cpu0[0]) / wall,
        "s3": s3,
        "deleg": deleg,
        "table_at_start": table_start,
        "table_at_end": table_end,
        "split_after_s": split_after,
        "split_fired": config == 'b' && (split_after.is_some() || is_split(&table_end)),
        "placed_whole": config != 'c' && config != 'd'
            && table_start.iter().chain(&table_end).any(|(r, _)| r.is_empty()),
        "converged": converged,
        "views": views,
    });
    eprintln!(
        "    {NAME}: {dir}: {total} ops in {wall:.1}s = {:.0} ops/s, p50 {:.2} p99 {:.2} ms, \
         errors {errors_total}, root cpu {:.0}%, table {table_start:?} -> {table_end:?}",
        total as f64 / wall,
        r["latency"]["p50_ms"].as_f64().unwrap_or(f64::NAN),
        r["latency"]["p99_ms"].as_f64().unwrap_or(f64::NAN),
        r["root_cpu_pct"].as_f64().unwrap_or(f64::NAN),
    );
    Ok(r)
}

fn append(out: &std::path::Path, v: &Value) -> Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out)
        .with_context(|| format!("opening {}", out.display()))?;
    writeln!(f, "{}", serde_json::to_string(v)?)?;
    Ok(())
}

pub fn dirsplit_bench(_seed: u64) -> Result<()> {
    let spec = Spec::from_env()?;
    eprintln!("    {NAME}: {spec:?}");
    let split = if spec.configs == ['b'] { "20" } else { "0" };
    let mut cl = build_cluster(&spec, split)?;
    let result = (|| -> Result<()> {
        let ids: Vec<u64> = cl.clients.iter().map(node_id).collect::<Result<_>>()?;
        // Backups and link measurements settle (the backup candidates
        // need a stable RTT for a while).
        std::thread::sleep(Duration::from_secs(8));
        let mut configs = spec.configs.clone();
        let k = spec.rotate % configs.len();
        configs.rotate_left(k);
        let mut seq = 0;
        for w in &spec.workloads {
            for c in &configs {
                seq += 1;
                let late = refresh_netem(&spec, &cl.clients, &mut cl.netem)?;
                if late > 0 {
                    eprintln!("    {NAME}: {late} late UDP port(s) placed before phase {seq}");
                }
                let r = run_phase(&spec, &cl, &ids, seq, w, *c);
                match r {
                    Ok(v) => append(&spec.out, &v)?,
                    Err(e) => {
                        let v = json!({
                            "label": spec.label, "rep": spec.rep, "writers": spec.writers,
                            "workload": w, "config": c.to_string(), "seq": seq,
                            "failed": format!("{e:#}"),
                        });
                        append(&spec.out, &v)?;
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    })();
    super::m11::dump_logs_on_failure(NAME, &cl.clients, &result);
    unmount_all(&mut cl.clients);
    cl.netem.teardown();
    result
}
