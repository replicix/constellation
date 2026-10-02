//! A P2P link must survive local network interfaces coming and going.
//!
//! On a shared build host every daemon used to advertise one address per
//! local interface — including every docker bridge, and other jobs create
//! and remove those every few seconds. Two nodes on such a host talk over
//! whichever of those addresses iroh happened to select; when that bridge
//! was removed the link stalled for 60–120 s (once ~7 min), long enough to
//! turn a 4 s lock grant into 451 s.
//!
//! This test rebuilds that in a private network namespace (so it never
//! touches the host's interfaces): a default route into a dummy device
//! (packets to a vanished address leave and are lost, as the EC2 VPC
//! gateway drops them), two stable interfaces, and bridges named the way
//! docker names them. Two in-process daemons link up, pinging each other
//! every 100 ms and gossiping every 200 ms in both directions, then
//!
//! 1. every bridge is removed and others come and go every 500 ms: the
//!    link must not notice ([`CHURN_BOUND`]);
//! 2. an admitted interface the link does not use comes and goes: the
//!    pooled connections must survive it (not be reset);
//! 3. the interface carrying the link's selected path is deleted while
//!    another stable address remains (twice): both directions must recover
//!    within [`BOUND`].
//!
//! The behaviour before the fix is reproduced with
//!
//! ```text
//! CONSTELLATION_P2P_INTERFACES_DENY=none CONSTELLATION_P2P_PATH_IDLE_MS=15000 \
//! CONSTELLATION_P2P_DIAL_TIMEOUT_MS=600000 CONSTELLATION_P2P_TEST_NO_ADDR_WATCH=1 \
//!   cargo test -p constellation-net --test addr_churn -- --nocapture
//! ```
//!
//! every interface advertised (the path selector then ranks every direct
//! path alike, which is iroh's own biased-RTT choice), iroh's 15 s path
//! idle timeout, an effectively unbounded dial, and no interface watch.
//!
//! It needs root in a fresh netns: the outer test re-runs this binary
//! under `sudo -n ip netns exec`, and is skipped (with a message) on a
//! host without passwordless sudo or `ip`.

use constellation_net::{run_gossip, P2p, Payload, PeerService, Peers, RelayPolicy};
use std::net::IpAddr;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const INNER_ENV: &str = "CONSTELLATION_NET_TEST_ADDR_CHURN_INNER";
const A: u64 = 1;
const B: u64 = 2;
/// How fast a link with another usable address must recover after the
/// address it was using disappears.
const BOUND: Duration = Duration::from_secs(2);
/// Bridges coming and going that the link does not use must not disturb
/// it at all: one failed 500 ms ping plus its retry is the most allowed.
const CHURN_BOUND: Duration = Duration::from_secs(1);
/// How long to keep measuring past the bound, so a failure reports how
/// long recovery really took.
const MEASURE: Duration = Duration::from_secs(150);
const BRIDGES: u8 = 6;
/// Address-loss rounds (the lost interface is re-created between them).
const LOSS_ROUNDS: usize = 2;
const STABLE: [(&str, &str); 2] = [("cnet-a", "10.99.0.2/24"), ("cnet-b", "10.98.0.2/24")];
/// An admitted interface that appears after the nodes published their
/// addresses, so the link never selects it.
const SPARE: (&str, &str) = ("cnet-c", "10.97.0.2/24");

#[derive(Default)]
struct Recorder {
    node: u64,
    gossip: Mutex<Vec<Instant>>,
}

impl PeerService for Recorder {
    fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {
        self.gossip.lock().unwrap().push(Instant::now());
    }
    fn lease_requested(
        &self,
        part: String,
        _requester: u64,
        _epoch_applied: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::LeaseHandoff {
                part,
                epoch: 1,
                released: false,
                etag: None,
                head_seq: None,
            }
        })
    }
    fn node_id(&self) -> u64 {
        self.node
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

/// Runs `ip` commands for the inner test. One shell, spawned before any
/// endpoint exists, forks them all: a fork from this process copies its
/// file descriptors, and a child still between fork and exec holds a copy
/// of an iroh socket that iroh is closing and re-binding on the same port
/// (it does that on every interface change), failing the bind. Docker is
/// not a child of the daemon either.
struct Shell {
    /// Ends when its stdin closes, with this process.
    _child: std::process::Child,
    stdin: std::process::ChildStdin,
    stdout: std::io::Lines<std::io::BufReader<std::process::ChildStdout>>,
}

static SHELL: Mutex<Option<Shell>> = Mutex::new(None);

/// Each input line is run by the shell; it answers with one line, `ok`
/// or `fail`, then the command's output with newlines turned into `|`.
const SHELL_LOOP: &str = r#"while read -r line; do
  if out=$(eval "$line" 2>&1); then st=ok; else st=fail; fi
  printf '%s %s\n' "$st" "$(printf %s "$out" | tr '\n' '|')"
done"#;

fn start_shell() {
    let mut child = Command::new("sh")
        .args(["-c", SHELL_LOOP])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = std::io::BufRead::lines(std::io::BufReader::new(child.stdout.take().unwrap()));
    *SHELL.lock().unwrap() = Some(Shell {
        _child: child,
        stdin,
        stdout,
    });
}

/// Run `ip <args>` through the shell and return its output.
fn ip(args: &str) -> String {
    use std::io::Write;
    let mut shell = SHELL.lock().unwrap();
    let shell = shell.as_mut().expect("start_shell first");
    writeln!(shell.stdin, "ip {args}").unwrap();
    let answer = shell.stdout.next().unwrap().unwrap();
    let (status, out) = answer.split_once(' ').unwrap_or((&answer, ""));
    let out = out.replace('|', "\n");
    assert_eq!(status, "ok", "ip {args} failed: {out}");
    out
}

fn sudo_ok() -> bool {
    let sudo = Command::new("sudo").args(["-n", "true"]).status();
    let iproute = Command::new("ip").arg("-V").output();
    matches!(sudo, Ok(s) if s.success()) && matches!(iproute, Ok(o) if o.status.success())
}

/// Deletes the namespace however the test ends.
struct Netns(String);

impl Drop for Netns {
    fn drop(&mut self) {
        let _ = Command::new("sudo")
            .args(["-n", "ip", "netns", "del", &self.0])
            .status();
    }
}

#[test]
fn a_link_recovers_within_seconds_when_its_address_disappears() {
    if std::env::var(INNER_ENV).is_ok() {
        return; // we are the inner run
    }
    if !sudo_ok() {
        eprintln!("SKIPPED: needs passwordless sudo and iproute2 for a private netns");
        return;
    }
    let ns = Netns(format!("cnet-churn-{}", std::process::id()));
    let added = Command::new("sudo")
        .args(["-n", "ip", "netns", "add", &ns.0])
        .status()
        .unwrap();
    assert!(added.success(), "could not create netns {}", ns.0);
    let mut cmd = Command::new("sudo");
    cmd.args(["-n", "ip", "netns", "exec", &ns.0, "env"])
        .arg(format!("{INNER_ENV}=1"));
    // RUST_LOG, and the P2P knobs (to measure other settings).
    for (key, value) in std::env::vars() {
        if key == "RUST_LOG" || key.starts_with("CONSTELLATION_P2P") {
            cmd.arg(format!("{key}={value}"));
        }
    }
    let status = cmd
        .arg(std::env::current_exe().unwrap())
        .args(["addr_churn_inner", "--exact", "--nocapture"])
        .status()
        .unwrap();
    assert!(status.success(), "the in-netns run failed (see above)");
}

/// The netns as a docker host looks: loopback; a default route into a
/// dummy device, so a packet to an address that no longer exists here
/// leaves and is lost (an EC2 VPC drops it the same way); two stable
/// interfaces; and docker's bridges, up as when a container is attached.
fn build_network() {
    start_shell();
    ip("link set lo up");
    ip("link add uplink type dummy");
    ip("link set uplink up");
    ip("route add default dev uplink");
    for (name, addr) in STABLE {
        add_stable(name, addr);
    }
    for n in 0..BRIDGES {
        add_bridge(n);
    }
}

fn add_stable(name: &str, addr: &str) {
    ip(&format!("link add {name} type dummy"));
    ip(&format!("addr add {addr} dev {name}"));
    ip(&format!("link set {name} up"));
}

fn add_bridge(n: u8) {
    ip(&format!("link add br-cnet{n} type bridge"));
    ip(&format!("addr add 10.200.{n}.1/24 dev br-cnet{n}"));
    ip(&format!("link set br-cnet{n} up"));
}

/// The interface holding `addr` in this netns.
fn interface_of(addr: IpAddr) -> Option<String> {
    let needle = format!(" {addr}/");
    ip("-o addr show")
        .lines()
        .find(|line| line.contains(&needle))
        .and_then(|line| line.split_whitespace().nth(1))
        .map(str::to_string)
}

struct Node {
    peers: Peers,
    recorder: Arc<Recorder>,
    pub_hex: String,
    addr: iroh::EndpointAddr,
}

async fn node(id: u64) -> Node {
    let topic = constellation_net::topic_for(Some(&[7u8; 32]), "addr-churn");
    let p2p = P2p::spawn_with(iroh::SecretKey::generate(), topic, RelayPolicy::Disabled)
        .await
        .unwrap();
    println!("node {id}: every address {:?}", p2p.addr().addrs);
    // What the daemon publishes in the registry.
    let (addr, pub_hex) = (p2p.advertised_addr().await, p2p.pubkey_hex());
    println!("node {id}: advertises {:?}", addr.addrs);
    let recorder = Arc::new(Recorder {
        node: id,
        ..Default::default()
    });
    let peers = Peers::new(p2p, id);
    {
        let (peers, recorder) = (peers.clone(), recorder.clone());
        tokio::spawn(async move { peers.serve(recorder).await });
    }
    Node {
        peers,
        recorder,
        pub_hex,
        addr,
    }
}

fn enroll(nodes: &[&Node]) -> Vec<(u64, String, serde_json::Value)> {
    nodes
        .iter()
        .map(|n| {
            (
                n.recorder.node,
                n.pub_hex.clone(),
                serde_json::to_value(&n.addr).unwrap(),
            )
        })
        .collect()
}

/// One direction of the link as one node sees it: when its pings to the
/// other answered, and when the other's gossip arrived.
struct Direction {
    label: &'static str,
    pings: Arc<Mutex<Vec<Instant>>>,
    gossip: Arc<Recorder>,
}

impl Direction {
    /// Ping `to` every 100 ms for the rest of the test, each bounded by
    /// the lease path's 500 ms, like a holder's forwards.
    fn watch(label: &'static str, from: &Node, to: u64) -> Self {
        let pings = Arc::new(Mutex::new(Vec::new()));
        {
            let (peers, pings) = (from.peers.clone(), pings.clone());
            tokio::spawn(async move {
                loop {
                    if peers.ping_node(to).await {
                        pings.lock().unwrap().push(Instant::now());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        }
        Self {
            label,
            pings,
            gossip: from.recorder.clone(),
        }
    }

    /// The longest stretch within `[from, to]` with no answered ping, and
    /// the longest with no gossip received.
    fn outage(&self, from: Instant, to: Instant) -> (Duration, Duration) {
        (
            max_gap(&self.pings.lock().unwrap(), from, to),
            max_gap(&self.gossip.gossip.lock().unwrap(), from, to),
        )
    }

    /// Healthy over the last 2 s: pings answered and gossip arriving,
    /// neither with a gap over 600 ms.
    fn healthy(&self) -> bool {
        let now = Instant::now();
        let (ping, gossip) = self.outage(now - Duration::from_secs(2), now);
        ping < Duration::from_millis(600) && gossip < Duration::from_millis(600)
    }
}

fn max_gap(times: &[Instant], from: Instant, to: Instant) -> Duration {
    let mut last = from;
    let mut worst = Duration::ZERO;
    for &t in times.iter().filter(|&&t| t > from && t <= to) {
        worst = worst.max(t - last);
        last = t;
    }
    worst.max(to.saturating_duration_since(last))
}

/// Wait until every direction has been healthy for 2 s, or `limit`.
async fn settle(dirs: &[&Direction], limit: Duration) {
    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    while started.elapsed() < limit && !dirs.iter().all(|d| d.healthy()) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn report(phase: &str, dirs: &[&Direction], from: Instant, to: Instant) -> Duration {
    let mut worst = Duration::ZERO;
    for d in dirs {
        let (ping, gossip) = d.outage(from, to);
        println!(
            "{phase}: {}: longest without an answered ping {ping:?}, without gossip {gossip:?}",
            d.label
        );
        worst = worst.max(ping).max(gossip);
    }
    worst
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn addr_churn_inner() {
    if std::env::var(INNER_ENV).is_err() {
        return; // only inside the netns the outer test made
    }
    init_tracing();
    build_network();
    let a = node(A).await;
    let b = node(B).await;
    let records = enroll(&[&a, &b]);
    a.peers.refresh_registry(records.clone());
    b.peers.refresh_registry(records);
    let rx = a.peers.join_topic(vec![b.addr.id]).await.unwrap();
    tokio::spawn(run_gossip(a.peers.clone(), rx, a.recorder.clone()));
    let rx = b.peers.join_topic(vec![a.addr.id]).await.unwrap();
    tokio::spawn(run_gossip(b.peers.clone(), rx, b.recorder.clone()));
    // Both gossip every 200 ms, like a busy holder announcing segments.
    for n in [&a, &b] {
        let peers = n.peers.clone();
        tokio::spawn(async move {
            let mut seq = 0u64;
            loop {
                let _ = peers
                    .gossip(Payload::SegmentPublished {
                        part: "p".into(),
                        seq,
                        epoch: 1,
                    })
                    .await;
                seq += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }
    let ab = Direction::watch("A -> B", &a, B);
    let ba = Direction::watch("B -> A", &b, A);
    let dirs = [&ab, &ba];
    settle(&dirs, Duration::from_secs(20)).await;
    assert!(dirs.iter().all(|d| d.healthy()), "the link never came up");
    println!("A -> B paths: {:?}", a.peers.path_summary(B));
    println!("B -> A paths: {:?}", b.peers.path_summary(A));

    // 1. Docker churn: every bridge goes away, others come and go.
    let from = Instant::now();
    for n in 0..BRIDGES {
        ip(&format!("link del br-cnet{n}"));
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    for round in 0..12u8 {
        let n = 100 + round % 3;
        add_bridge(n);
        tokio::time::sleep(Duration::from_millis(250)).await;
        ip(&format!("link del br-cnet{n}"));
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let churn = report("bridge churn", &dirs, from, Instant::now());
    settle(&dirs, MEASURE).await;
    let churn_end = Instant::now();
    let churn_total = report("bridge churn, until healthy", &dirs, from, churn_end);

    // 2. An admitted address the link does not use comes and goes: its
    //    loss must not reset the connections that never used it.
    add_stable(SPARE.0, SPARE.1);
    settle(&dirs, MEASURE).await;
    let before = (
        a.peers.pooled_connection_id(B).await,
        b.peers.pooled_connection_id(A).await,
    );
    println!(
        "spare: A -> B paths: {:?}; B -> A {:?}",
        a.peers.path_summary(B),
        b.peers.path_summary(A)
    );
    let from = Instant::now();
    ip(&format!("link del {}", SPARE.0));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let spare = report("unused address loss", &dirs, from, Instant::now());
    let after = (
        a.peers.pooled_connection_id(B).await,
        b.peers.pooled_connection_id(A).await,
    );
    println!("spare: pooled connection ids before {before:?}, after {after:?}");
    settle(&dirs, MEASURE).await;

    // 3. An advertised address the link is using disappears; the other
    //    one is still there. Twice, re-creating it in between.
    let mut loss = Duration::ZERO;
    for round in 0..LOSS_ROUNDS {
        let summary = a.peers.path_summary(B).expect("A pools a connection to B");
        println!("round {round}: A -> B paths before: {summary:?}");
        let selected = summary.selected_addr.expect("a direct path is selected");
        let gone = interface_of(selected.ip()).expect("the selected address is local");
        println!("round {round}: deleting {gone} (holds {selected}, the selected path)");
        let from = Instant::now();
        ip(&format!("link del {gone}"));
        settle(&dirs, MEASURE).await;
        loss = loss.max(report(
            &format!("address loss {round}"),
            &dirs,
            from,
            Instant::now(),
        ));
        println!(
            "round {round}: A -> B paths after: {:?}; B -> A {:?}",
            a.peers.path_summary(B),
            b.peers.path_summary(A)
        );
        if let Some((name, addr)) = STABLE.iter().find(|(name, _)| *name == gone) {
            add_stable(name, addr);
            settle(&dirs, MEASURE).await;
        }
    }

    assert!(
        churn <= CHURN_BOUND && churn_total <= CHURN_BOUND,
        "bridge churn stalled the link for {:?} (bound {CHURN_BOUND:?})",
        churn.max(churn_total)
    );
    assert!(
        before.0.is_some() && before.1.is_some() && before == after,
        "losing an address no connection used replaced a pooled connection: {before:?} -> {after:?}"
    );
    assert!(
        spare <= CHURN_BOUND,
        "losing an address no connection used stalled the link for {spare:?} (bound {CHURN_BOUND:?})"
    );
    assert!(
        loss <= BOUND,
        "the link took {loss:?} to recover after its address vanished (bound {BOUND:?})"
    );
}
