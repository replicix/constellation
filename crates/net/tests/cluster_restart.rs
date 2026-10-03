//! A whole-cluster restart (`git-under-flock-faults`' "kill -9 of the
//! whole cluster"): every node dies at once and comes back one after the
//! other, each under its own persisted node key at a new port.
//!
//! The node that comes back first reads the registry while its peers are
//! still down, so all it knows of them is their dead incarnations'
//! addresses, and its gossip bootstrap dials those at once. A peer that
//! restarts while such a dial is still in its handshake must be reachable
//! in both directions within a few seconds of coming back: its requests
//! to the early node answered, and the early node's requests to it.
//!
//! This restart order was the first suspect for `p2p-restart-auth`, and
//! the test stays as a guard for it. It passes both with and without
//! that fix: the stall also needed a UDP socket rebind (a link change on
//! the host), which an in-process test cannot cause. The rebind's lost
//! wakeup is tested in `udp_rebind_wakeups.rs`, and the whole cluster in
//! the `p2p-cluster-restart` harness scenario.

use constellation_net::{run_gossip, P2p, Payload, PeerService, Peers, RelayPolicy};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROOT: u64 = 1;
const PEER: u64 = 2;
/// How fast both directions must work after the peer's restart.
const BOUND: Duration = Duration::from_secs(5);
/// How long to keep measuring past the bound, so a failure says how long
/// recovery really took.
const MEASURE: Duration = Duration::from_secs(45);

struct Service(u64);

impl PeerService for Service {
    fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {}
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
        self.0
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

fn topic_id() -> iroh_gossip::proto::TopicId {
    constellation_net::topic_for(Some(&[7u8; 32]), "cluster-restart-test")
}

fn enrollment(
    node: u64,
    key: &iroh::SecretKey,
    addr: &iroh::EndpointAddr,
) -> (u64, String, serde_json::Value) {
    (
        node,
        constellation_net::pubkey_hex(&key.public()),
        serde_json::to_value(addr).unwrap(),
    )
}

/// A daemon-like node: serving, enrolled peers, in the gossip topic.
async fn start(
    node: u64,
    key: &iroh::SecretKey,
    other: u64,
    other_key: &iroh::SecretKey,
    other_addr: &iroh::EndpointAddr,
) -> (Peers, iroh::EndpointAddr) {
    let p2p = P2p::spawn_with(key.clone(), topic_id(), RelayPolicy::Disabled)
        .await
        .unwrap();
    let addr = p2p.addr();
    let peers = Peers::new(p2p, node);
    peers.refresh_registry(vec![
        enrollment(node, key, &addr),
        enrollment(other, other_key, other_addr),
    ]);
    let service = Arc::new(Service(node));
    {
        let (peers, service) = (peers.clone(), service.clone());
        tokio::spawn(async move { peers.serve(service).await });
    }
    let rx = peers.join_topic(vec![other_addr.id]).await.unwrap();
    tokio::spawn(run_gossip(peers.clone(), rx, service));
    (peers, addr)
}

/// Time until a ping from `from` to `to` is answered, pinging every
/// 100 ms as a busy daemon's requests would.
async fn time_to_ping(from: &Peers, to: u64, since: Instant) -> Option<Duration> {
    while since.elapsed() < MEASURE {
        if from.ping_node(to).await {
            return Some(since.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_restarting_during_the_first_nodes_dial_is_reachable_promptly() {
    init_tracing();
    let root_key = iroh::SecretKey::generate();
    let peer_key = iroh::SecretKey::generate();

    // The peer's previous incarnation: its address is all the registry
    // has when the root comes back. It is gone (its port closed).
    let old = P2p::spawn_with(peer_key.clone(), topic_id(), RelayPolicy::Disabled)
        .await
        .unwrap();
    let old_addr = old.addr();
    old.endpoint().close().await;
    drop(old);

    // The root restarts first: it bootstraps gossip from the dead address.
    let (root, root_addr) = start(ROOT, &root_key, PEER, &peer_key, &old_addr).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // The peer restarts under the same key, reads the registry (the
    // root's new address) and dials the root.
    let (peer, peer_addr) = start(PEER, &peer_key, ROOT, &root_key, &root_addr).await;
    let restarted = Instant::now();
    // The root's next registry read carries the peer's new address.
    root.refresh_registry(vec![
        enrollment(ROOT, &root_key, &root_addr),
        enrollment(PEER, &peer_key, &peer_addr),
    ]);

    let (to_root, to_peer) = tokio::join!(
        time_to_ping(&peer, ROOT, restarted),
        time_to_ping(&root, PEER, restarted)
    );
    eprintln!("cluster restart: peer->root ping after {to_root:?}, root->peer after {to_peer:?}");
    for (what, took) in [("peer -> root", to_root), ("root -> peer", to_peer)] {
        let took = took.unwrap_or_else(|| panic!("{what}: never answered within {MEASURE:?}"));
        assert!(
            took <= BOUND,
            "{what}: took {took:?} after the peer's restart (bound {BOUND:?})"
        );
    }
}
