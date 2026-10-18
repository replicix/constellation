//! A peer that is SIGKILLed and restarted with the same node key (the
//! same iroh `EndpointId`) must become reachable again from the nodes
//! that knew its previous incarnation, promptly — not after the QUIC
//! idle timeout of the dead connection they still hold.
//!
//! The victim runs in a child process (this same test binary, re-run
//! with an env var selecting [`restart_victim_child`]) so a real
//! `SIGKILL` takes it down: no CONNECTION_CLOSE, no graceful anything,
//! exactly what a crashed daemon leaves behind. The observer stays in
//! this process and behaves like a daemon: it keeps the victim enrolled,
//! pings it on a ticker, and consumes the gossip topic.
//!
//! A *planned* handover (the in-place upgrade's `exec`) is the other
//! way a node is replaced under the same key: the old incarnation is
//! alive until it goes and can say goodbye. The victim then closes its
//! connections (`Peers::close_for_successor`) on a command from the
//! parent, or exits without (the control).

use constellation_net::{run_gossip, P2p, Payload, PeerService, Peers, RelayPolicy};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const VICTIM_KEY_ENV: &str = "CONSTELLATION_NET_TEST_VICTIM_KEY";
const OBSERVER_KEY_ENV: &str = "CONSTELLATION_NET_TEST_OBSERVER_PUB";
const OBSERVER_ADDR_ENV: &str = "CONSTELLATION_NET_TEST_OBSERVER_ADDR";
const INCARNATION_ENV: &str = "CONSTELLATION_NET_TEST_INCARNATION";
const OBSERVER: u64 = 1;
const VICTIM: u64 = 2;
/// How fast the observer must reach the restarted victim again.
const BOUND: Duration = Duration::from_secs(5);
/// How long to keep measuring past the bound, so a failure reports how
/// long recovery really took (before the fix: the QUIC idle timeout).
const MEASURE: Duration = Duration::from_secs(45);

#[derive(Default)]
struct Recorder {
    node: u64,
    segments: Mutex<Vec<(String, Instant)>>,
    /// When each request (a lease request, which every victim
    /// incarnation sends once at start) arrived, like a delegate's first
    /// renewal after a resume.
    requests: Mutex<Vec<Instant>>,
}

impl PeerService for Recorder {
    fn segment_published(&self, part: &str, _seq: u64, _epoch: u64) {
        self.segments
            .lock()
            .unwrap()
            .push((part.to_string(), Instant::now()));
    }
    fn lease_requested(
        &self,
        part: String,
        _requester: u64,
        _epoch_applied: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        self.requests.lock().unwrap().push(Instant::now());
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

/// `RUST_LOG=...` shows the transport's view when debugging this test.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex32(s: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap();
    }
    out
}

fn enrollment(
    node: u64,
    p2p_pub: &str,
    addr: &iroh::EndpointAddr,
) -> (u64, String, serde_json::Value) {
    (
        node,
        p2p_pub.to_string(),
        serde_json::to_value(addr).unwrap(),
    )
}

/// The victim's body. A no-op unless the parent test selected it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_victim_child() {
    let Ok(key_hex) = std::env::var(VICTIM_KEY_ENV) else {
        return;
    };
    init_tracing();
    let observer_pub = std::env::var(OBSERVER_KEY_ENV).unwrap();
    let observer_addr: iroh::EndpointAddr =
        serde_json::from_str(&std::env::var(OBSERVER_ADDR_ENV).unwrap()).unwrap();
    let incarnation = std::env::var(INCARNATION_ENV).unwrap();
    let key = iroh::SecretKey::from_bytes(&unhex32(&key_hex));
    let p2p = P2p::spawn_with(key, topic_id(), RelayPolicy::Disabled)
        .await
        .unwrap();
    let addr = p2p.addr();
    let own_pub = p2p.pubkey_hex();
    let peers = Peers::new(p2p, VICTIM);
    peers.refresh_registry(vec![
        enrollment(OBSERVER, &observer_pub, &observer_addr),
        enrollment(VICTIM, &own_pub, &addr),
    ]);
    let service = Arc::new(Recorder {
        node: VICTIM,
        ..Default::default()
    });
    {
        let (peers, service) = (peers.clone(), service.clone());
        tokio::spawn(async move { peers.serve(service).await });
    }
    // Like the daemon: join the topic bootstrapping from the registry
    // at once, and probe peers every 5 s (the first probe after 5 s).
    let rx = peers.join_topic(vec![observer_addr.id]).await.unwrap();
    tokio::spawn(run_gossip(peers.clone(), rx, service));
    {
        let peers = peers.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                peers.probe_all().await;
                let up = peers
                    .snapshot()
                    .iter()
                    .any(|p| p.node_id == OBSERVER && p.connected);
                eprintln!("victim probe: observer connected={up}");
            }
        });
    }
    // Commands from the parent, one per line: `CLOSE` says goodbye as a
    // handover does (bounded, as the engine bounds it) and exits; `EXIT`
    // exits at once without (an `exec` closes the socket the same way).
    {
        let (peers, handle) = (peers.clone(), tokio::runtime::Handle::current());
        std::thread::spawn(move || {
            for line in std::io::stdin().lock().lines() {
                match line.unwrap_or_default().as_str() {
                    "CLOSE" => {
                        let closed = handle.block_on(async {
                            tokio::time::timeout(
                                Duration::from_millis(500),
                                peers.close_for_successor(),
                            )
                            .await
                        });
                        eprintln!("victim closed for its successor: {closed:?}");
                        std::process::exit(0);
                    }
                    "EXIT" => std::process::exit(0),
                    _ => {}
                }
            }
        });
    }
    // At once, like a delegate's first renewal after a resume: one
    // request to the observer.
    {
        let peers = peers.clone();
        tokio::spawn(async move {
            let hello = Payload::LeaseRequest {
                part: "hello".into(),
                requester: VICTIM,
                epoch_applied: None,
            };
            let _ = peers.request_to_node(OBSERVER, &hello).await;
        });
    }
    println!("VICTIM_ADDR {}", serde_json::to_string(&addr).unwrap());
    let mut seq = 0u64;
    loop {
        seq += 1;
        let _ = peers
            .gossip(Payload::SegmentPublished {
                part: format!("inc-{incarnation}"),
                seq,
                epoch: 1,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn topic_id() -> iroh_gossip::proto::TopicId {
    constellation_net::topic_for(Some(&[42u8; 32]), "restart-test")
}

struct Victim {
    child: Child,
    stdin: ChildStdin,
    addr: iroh::EndpointAddr,
}

impl Victim {
    /// Send the child a command and wait for it to exit.
    fn command_and_wait(&mut self, command: &str) {
        writeln!(self.stdin, "{command}").unwrap();
        self.stdin.flush().unwrap();
        let status = self.child.wait().unwrap();
        assert!(status.success(), "the victim exited with {status}");
    }
}

fn spawn_victim(
    key: &iroh::SecretKey,
    observer_pub: &str,
    observer_addr: &iroh::EndpointAddr,
    incarnation: u32,
) -> Victim {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["restart_victim_child", "--exact", "--nocapture"])
        .env(VICTIM_KEY_ENV, hex(&key.to_bytes()))
        .env(OBSERVER_KEY_ENV, observer_pub)
        .env(
            OBSERVER_ADDR_ENV,
            serde_json::to_string(observer_addr).unwrap(),
        )
        .env(INCARNATION_ENV, incarnation.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let addr = loop {
        let line = lines
            .next()
            .expect("victim exited before announcing its address")
            .unwrap();
        if let Some(json) = line.strip_prefix("VICTIM_ADDR ") {
            break serde_json::from_str(json).unwrap();
        }
    };
    // Keep draining so the child never blocks on a full pipe.
    std::thread::spawn(move || for _ in lines.by_ref() {});
    Victim { child, stdin, addr }
}

/// Time until the observer reaches the victim again, on every channel
/// the daemon relies on: a direct request (ping), the peer directory's
/// `connected` flag, and gossip from the new incarnation.
async fn time_to_reach(
    peers: &Peers,
    recorder: &Recorder,
    incarnation: u32,
    since: Instant,
) -> (Option<Duration>, Option<Duration>, Option<Duration>) {
    let part = format!("inc-{incarnation}");
    let (mut ping, mut connected, mut gossip) = (None, None, None);
    while since.elapsed() < MEASURE && (ping.is_none() || connected.is_none() || gossip.is_none()) {
        // Like `probe_all` on the daemon's registry ticker, but denser.
        if ping.is_none() && peers.ping_node(VICTIM).await {
            ping = Some(since.elapsed());
        }
        if connected.is_none()
            && peers
                .snapshot()
                .iter()
                .any(|p| p.node_id == VICTIM && p.connected)
        {
            connected = Some(since.elapsed());
        }
        if gossip.is_none() {
            if let Some((_, at)) = recorder
                .segments
                .lock()
                .unwrap()
                .iter()
                .find(|(p, _)| *p == part)
            {
                gossip = Some(at.saturating_duration_since(since));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (ping, connected, gossip)
}

/// The observing node: a daemon-like peer that knows the victim.
struct Observer {
    peers: Peers,
    recorder: Arc<Recorder>,
    pub_hex: String,
    addr: iroh::EndpointAddr,
    victim_pub: String,
}

impl Observer {
    async fn start(victim_key: &iroh::SecretKey) -> Self {
        init_tracing();
        let p2p = P2p::spawn_with(
            iroh::SecretKey::generate(),
            topic_id(),
            RelayPolicy::Disabled,
        )
        .await
        .unwrap();
        let (addr, pub_hex) = (p2p.addr(), p2p.pubkey_hex());
        let recorder = Arc::new(Recorder {
            node: OBSERVER,
            ..Default::default()
        });
        let peers = Peers::new(p2p, OBSERVER);
        {
            let (peers, recorder) = (peers.clone(), recorder.clone());
            tokio::spawn(async move { peers.serve(recorder).await });
        }
        Self {
            peers,
            recorder,
            pub_hex,
            addr,
            victim_pub: constellation_net::pubkey_hex(&victim_key.public()),
        }
    }

    /// What a registry refresh does once the victim's record changed.
    fn enroll(&self, victim: &iroh::EndpointAddr) {
        self.peers.refresh_registry(vec![
            enrollment(OBSERVER, &self.pub_hex, &self.addr),
            enrollment(VICTIM, &self.victim_pub, victim),
        ]);
    }

    fn spawn_victim(&self, key: &iroh::SecretKey, incarnation: u32) -> Victim {
        let victim = spawn_victim(key, &self.pub_hex, &self.addr, incarnation);
        self.enroll(&victim.addr);
        victim
    }

    async fn join_gossip(&self, victim: &Victim) {
        let rx = self.peers.join_topic(vec![victim.addr.id]).await.unwrap();
        tokio::spawn(run_gossip(self.peers.clone(), rx, self.recorder.clone()));
    }

    /// Incarnation `n` fully reachable: a warm, pooled connection.
    async fn reached(&self, incarnation: u32) {
        let (ping, connected, gossip) =
            time_to_reach(&self.peers, &self.recorder, incarnation, Instant::now()).await;
        assert!(
            ping.is_some() && connected.is_some() && gossip.is_some(),
            "incarnation {incarnation} never became reachable: \
             ping {ping:?} connected {connected:?} gossip {gossip:?}"
        );
        assert!(self.peers.connection_alive(VICTIM).await);
    }
}

fn crash(victim: &mut Victim) {
    victim.child.kill().unwrap(); // SIGKILL
    victim.child.wait().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_restarted_with_the_same_key_is_reachable_again_promptly() {
    if std::env::var(VICTIM_KEY_ENV).is_ok() {
        return; // we are the child
    }
    let victim_key = iroh::SecretKey::generate();
    let observer = Observer::start(&victim_key).await;
    let mut victim = observer.spawn_victim(&victim_key, 0);
    observer.join_gossip(&victim).await;
    observer.reached(0).await;

    // Crash it. The observer keeps probing through the outage, as the
    // daemon's ticker would.
    crash(&mut victim);
    let down = Instant::now();
    while down.elapsed() < Duration::from_secs(2) {
        assert!(
            !observer.peers.ping_node(VICTIM).await,
            "a dead peer answered"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Restart with the same key. The registry would carry the new
    // address within one refresh; hand it over at once so the transport
    // state is what is under test.
    let mut victim = observer.spawn_victim(&victim_key, 1);
    let restarted = Instant::now();
    let (ping, connected, gossip) =
        time_to_reach(&observer.peers, &observer.recorder, 1, restarted).await;
    eprintln!(
        "same-identity restart: observer reached the new incarnation: \
         ping {ping:?}, connected {connected:?}, gossip {gossip:?}"
    );
    crash(&mut victim);
    for (what, took) in [("ping", ping), ("connected", connected), ("gossip", gossip)] {
        let took = took.unwrap_or_else(|| panic!("{what}: never recovered within {MEASURE:?}"));
        assert!(
            took <= BOUND,
            "{what}: the restarted peer took {took:?} to be reachable again (bound {BOUND:?})"
        );
    }
}

/// The other half of plan 30 §M13's "slow is not gone": a peer that
/// crashed (and stays down) must stop counting as reachable soon after
/// a request to it goes unanswered, not only at the QUIC idle timeout
/// of the connection we pooled to it (which the unanswered requests
/// themselves kept from ever expiring).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_peer_stops_counting_as_reachable_promptly() {
    if std::env::var(VICTIM_KEY_ENV).is_ok() {
        return; // we are the child
    }
    let victim_key = iroh::SecretKey::generate();
    let observer = Observer::start(&victim_key).await;
    let mut victim = observer.spawn_victim(&victim_key, 0);
    observer.join_gossip(&victim).await;
    observer.reached(0).await;

    crash(&mut victim);
    let down = Instant::now();
    // One request, as the daemon's next registry tick would send.
    assert!(
        !observer.peers.ping_node(VICTIM).await,
        "a dead peer answered"
    );
    let mut gone = None;
    while down.elapsed() < MEASURE {
        if !observer.peers.connection_alive(VICTIM).await {
            gone = Some(down.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!("crashed peer: connection_alive turned false after {gone:?}");
    let took = gone.unwrap_or_else(|| panic!("still 'alive' {MEASURE:?} after the crash"));
    // One 500 ms request timeout, then the probe window.
    let bound = Duration::from_millis(500) + Duration::from_secs(3) + Duration::from_secs(2);
    assert!(took <= bound, "took {took:?} (bound {bound:?})");
}

/// How long the observer's requests to a handed-over peer's successor
/// may fail after the successor's own first request reached it: a round
/// trip (and a re-dial), not a probe window. The re-dial is a QUIC
/// handshake that a starved host stretches to several hundred ms (400 ms
/// seen with 16 copies of this test beside 32 busy threads); the other
/// outcome is the restart probe's 2 s (the control below needs 1.5 s), so
/// the bound sits between the two, not at a round trip's idle cost.
const HANDOVER_BOUND: Duration = Duration::from_millis(1_000);

/// A planned handover of the victim's identity: incarnation 0 either
/// says goodbye (`close`) or just goes, as an `exec` does; incarnation 1
/// starts under the same key and sends the observer a request at once.
/// Returns the time from that request's arrival to the observer's first
/// successful request to the victim, as a delegate's first renewal
/// waits for its root's answer.
async fn handover_to_successor(close: bool) -> Duration {
    let victim_key = iroh::SecretKey::generate();
    let observer = Observer::start(&victim_key).await;
    let mut victim = observer.spawn_victim(&victim_key, 0);
    observer.join_gossip(&victim).await;
    observer.reached(0).await;
    let asked_before = observer.recorder.requests.lock().unwrap().len();
    assert!(asked_before >= 1, "the first incarnation's request");

    victim.command_and_wait(if close { "CLOSE" } else { "EXIT" });
    let mut successor = observer.spawn_victim(&victim_key, 1);
    let waiting = Instant::now();
    let arrived = loop {
        if let Some(at) = observer
            .recorder
            .requests
            .lock()
            .unwrap()
            .get(asked_before)
            .copied()
        {
            break at;
        }
        assert!(
            waiting.elapsed() < MEASURE,
            "the successor's request never arrived"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let mut reached = None;
    while arrived.elapsed() < MEASURE {
        if observer.peers.ping_node(VICTIM).await {
            reached = Some(arrived.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    crash(&mut successor);
    reached.unwrap_or_else(|| panic!("the successor was never reachable within {MEASURE:?}"))
}

/// The planned handover's goodbye: the old incarnation closes its
/// connections before it exits, so the observer drops its pooled
/// connection to it at once and its next request reaches the successor
/// — it does not send into the dead incarnation's connection until the
/// restart probe's window (2 s) is over. This is what a delegate's first
/// renewal after an in-place upgrade needs: the root answers over its
/// pooled connection, and before the close that answer was lost for
/// 2.0-2.7 s (`delegate-handoff-renewal`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handed_over_peer_is_answered_within_a_round_trip() {
    if std::env::var(VICTIM_KEY_ENV).is_ok() {
        return; // we are the child
    }
    let took = handover_to_successor(true).await;
    eprintln!("handover with the close: first successful request {took:?} after the successor's");
    assert!(
        took <= HANDOVER_BOUND,
        "the observer took {took:?} to reach a successor whose predecessor closed its \
         connections (bound {HANDOVER_BOUND:?})"
    );
}

/// The control of the test above, and the probe it bypasses: a
/// predecessor that exits without closing (a crash, or the `exec`
/// before the close was added) leaves the observer sending into its
/// connection until the restart probe (2 s after the successor dialed
/// in) evicts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handed_over_peer_that_did_not_close_waits_for_the_probe() {
    if std::env::var(VICTIM_KEY_ENV).is_ok() {
        return; // we are the child
    }
    let took = handover_to_successor(false).await;
    eprintln!(
        "handover without the close: first successful request {took:?} after the successor's"
    );
    assert!(
        took >= Duration::from_millis(1_500),
        "the restart probe's window is gone: reached the successor after {took:?} \
         with nothing closed"
    );
}
