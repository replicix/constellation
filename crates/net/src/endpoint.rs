//! The iroh endpoint that carries the P2P fast path.
//!
//! One QUIC endpoint per daemon, keyed by the host node key. Peers learn
//! dial info from the filesystem's own node registry in S3, so the bucket
//! stays the only directory and the only trust root (DESIGN.md §8).
//! Relays are optional (`CONSTELLATION_P2P_RELAY`) and never replace the
//! registry: they only help when published direct addresses are not
//! mutually reachable (NAT / no shared L3).
//!
//! Everything here is best-effort. `spawn` returning an error, a peer that
//! never answers, and a gossip topic that never forms all degrade to the
//! S3 polling path that phases 1–2 already rely on.

use crate::addrs::{AddrPolicy, PreferredPaths};
use crate::allowlist::{Allowlist, Decision};
use crate::message::{ChunkDecline, ChunkStatus, Payload, Signed, ALPN};
use crate::relay::RelayPolicy;
use anyhow::{Context, Result};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

type ConnectionSlot = Arc<tokio::sync::Mutex<Option<iroh::endpoint::Connection>>>;
/// Called with a peer's endpoint id after its pooled connection was
/// found dead and evicted (see [`Pool::probe`]).
pub type EvictHook = Arc<dyn Fn(iroh::EndpointId) + Send + Sync>;

/// How long a probe of a pooled connection waits for any sign of life
/// after a request on it went unanswered. Only the peer's QUIC stack has
/// to answer (an ACK, or anything else), never its application, so a
/// peer that is merely slow to reply keeps its connection; a crashed or
/// restarted one cannot produce a single datagram on it.
pub(crate) const PROBE_WINDOW: Duration = Duration::from_secs(3);
/// The same when the peer itself just told us it may have restarted: it
/// dialed us afresh, or re-published its registry record. It is
/// provably up right now, so its QUIC stack answers on a live old
/// connection well within this.
const RESTART_PROBE_WINDOW: Duration = Duration::from_secs(2);
/// Poll interval for a probe's receive counter.
const PROBE_TICK: Duration = Duration::from_millis(25);

/// Default for `CONSTELLATION_P2P_DIAL_TIMEOUT_MS`: how long one dial
/// may take before the request that started it fails (its caller falls
/// back) and the next request dials afresh. A dial sends its Initials
/// to every known address of the peer at once, so on a direct link the
/// handshake takes one or two round trips; 5 s leaves room for a lossy
/// first flight (QUIC's first Initial retransmit is after about 1 s) and
/// for a relay handshake, but not for the 30 s idle timeout a dial to
/// vanished addresses would otherwise run into.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long [`P2p::advertised_addr`] waits for iroh's interface scan.
const ADDR_SCAN_WAIT: Duration = Duration::from_secs(2);
/// Default for `CONSTELLATION_P2P_PATH_IDLE_MS`: how long a network path
/// may go without receiving anything before noq abandons it, if the
/// connection has another path open. iroh's own default is 15 s, and
/// until then a path whose address vanished keeps carrying (and losing)
/// application data (see [`crate::addrs`]).
pub const PATH_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
/// iroh's ceiling for a path idle timeout (`PATH_MAX_IDLE_TIMEOUT`; it
/// clamps larger values with a warning). Its keepalive ceiling is a
/// third of it (5 s), so `path_idle / 3` is always accepted.
const PATH_IDLE_MAX: Duration = Duration::from_secs(15);

/// `CONSTELLATION_P2P_PATH_IDLE_MS`, else [`PATH_IDLE_TIMEOUT`]; clamped
/// to 300 ms ..= 15 s.
fn path_idle_timeout() -> Duration {
    ms_knob("CONSTELLATION_P2P_PATH_IDLE_MS", PATH_IDLE_TIMEOUT)
        .clamp(Duration::from_millis(300), PATH_IDLE_MAX)
}

/// `CONSTELLATION_P2P_DIAL_TIMEOUT_MS`, else [`DIAL_TIMEOUT`]; at least
/// 500 ms.
fn dial_timeout() -> Duration {
    ms_knob("CONSTELLATION_P2P_DIAL_TIMEOUT_MS", DIAL_TIMEOUT).max(Duration::from_millis(500))
}

fn ms_knob(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
}

/// iroh's transport settings with dead-path detection bounded by
/// `path_idle`: every path is pinged three times per idle period, so a
/// live one never idles out on a single lost keepalive. A path that
/// receives nothing for `path_idle` is abandoned (its in-flight data
/// moved to the other paths) — but only when another path is open: noq
/// refuses to abandon a connection's last path on a local timer
/// (`ClosePathError::LastOpenPath`; only the peer may). So this speeds
/// up the multipath case (two nodes on one host, a peer with several
/// addresses, a direct path next to a relay one); a connection down to
/// one path, the usual one between hosts, is still found dead only by
/// [`Pool::probe`] (3 s after a request goes unanswered) or by iroh's
/// 30 s connection idle timeout. Relay paths keep iroh's own longer
/// timeout (it sets one per relay path).
fn transport_config(path_idle: Duration) -> iroh::endpoint::QuicTransportConfig {
    iroh::endpoint::QuicTransportConfig::builder()
        .default_path_keep_alive_interval(path_idle / 3)
        .default_path_max_idle_timeout(path_idle)
        .build()
}

/// Why a pooled connection is being probed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Suspicion {
    /// A request on it was dropped unanswered (timed out or abandoned).
    Unanswered,
    /// The peer showed signs of a new incarnation: it opened a new
    /// connection to us, or re-published its registry record.
    Restarted,
}

/// The outbound connection pool, and what keeps it honest.
///
/// A pooled connection to a peer that was SIGKILLed stays "open" on our
/// side: nothing tells QUIC the other end is gone, and the peer's next
/// incarnation (same node key, so the same `EndpointId`) cannot close it
/// for us either — it has no state for that connection and usually a
/// new port. Requests over it simply never get an answer, and since the
/// callers bound them with timeouts that drop the request future, no
/// transport error ever evicted it. Left alone it lingers until the QUIC
/// idle timeout at best (and with the steady stream of new requests
/// keeping it busy, in practice much longer).
///
/// So anything suspicious — an unanswered request, the peer dialing in
/// afresh, its registry record changing — triggers a [`Pool::probe`]:
/// did the connection receive *anything* within a short window after we
/// sent it a ping? A live peer's QUIC stack ACKs within milliseconds
/// however busy its application is; a dead incarnation cannot. Only a
/// connection that stays silent is evicted, so a slow peer keeps its
/// connection (and `connection_alive` keeps reporting it reachable).
///
/// Evicting our own pooled connection is not enough, because of how
/// iroh routes a new dial. Its per-remote state keeps one *selected
/// path* (remote socket address) for the `EndpointId`, and sends every
/// new handshake's Initial packets to that path alone while any
/// connection to the remote is open. The dead incarnation's connections
/// (gossip's, and the ones it dialed to us) still carry its old address
/// with a frozen, flattering RTT, so the selection never moves to the
/// new incarnation's address, even once the new one has connected to
/// us — our re-dials go to a port nobody listens on. iroh re-selects
/// only on a new connection or path event, and clears the selection
/// only when the remote has no connection at all. So once the probe
/// proves the peer's old incarnation dead, *every* connection to it is
/// closed, whoever dialed it and whatever its ALPN (see [`Pool::tracked`]):
/// the selection resets, the re-dial sends its Initials to every known
/// address (the registry's current one included) and lands on the new
/// incarnation. The new incarnation's own fresh connections are closed
/// too — it re-dials on demand, and gossip re-forms its neighbor link.
struct Pool {
    connections: Mutex<HashMap<iroh::EndpointId, ConnectionSlot>>,
    /// Every connection the endpoint completed a handshake for, either
    /// side, any ALPN, by remote — weak handles, so this never keeps one
    /// alive. What [`Pool::evict`] closes.
    tracked: Mutex<HashMap<iroh::EndpointId, Vec<iroh::endpoint::WeakConnectionHandle>>>,
    /// Peers with a probe in flight. Single-flight per peer: a slow peer
    /// times out many requests at once, and one probe answers for all
    /// of them (a probe of a live connection ends at its first received
    /// datagram, so a slow peer costs at most one extra ping per
    /// unanswered request, never a pile of them).
    probing: Mutex<HashSet<iroh::EndpointId>>,
    /// Probes started, for tests.
    probes: std::sync::atomic::AtomicUsize,
    key: SecretKey,
    on_evict: Mutex<Option<EvictHook>>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Pool")
    }
}

impl Pool {
    fn new(key: SecretKey) -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            tracked: Mutex::new(HashMap::new()),
            probing: Mutex::new(HashSet::new()),
            probes: std::sync::atomic::AtomicUsize::new(0),
            key,
            on_evict: Mutex::new(None),
        }
    }

    fn gate(&self, id: &iroh::EndpointId) -> Option<ConnectionSlot> {
        self.connections.lock().unwrap().get(id).cloned()
    }

    /// Probe the connection pooled for `id`, if any, in the background.
    /// At most one probe per peer runs at a time.
    fn suspect(self: &Arc<Self>, id: iroh::EndpointId, why: Suspicion) {
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self.gate(&id).is_none() {
            return;
        }
        if !self.probing.lock().unwrap().insert(id) {
            return;
        }
        self.probes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pool = self.clone();
        rt.spawn(async move {
            pool.probe(id, why).await;
            pool.probing.lock().unwrap().remove(&id);
        });
    }

    async fn probe(&self, id: iroh::EndpointId, why: Suspicion) {
        let Some(gate) = self.gate(&id) else {
            return;
        };
        // Clone out rather than hold the slot: a probe must never block
        // a request's dial.
        let Some(conn) = gate.lock().await.clone() else {
            return;
        };
        let stable = conn.stable_id();
        if conn.close_reason().is_some() {
            // Closed cleanly (by the peer, or by us): nothing is stale
            // about the peer's address, so just stop handing it out.
            self.unpool(&gate, stable).await;
            return;
        }
        let window = match why {
            Suspicion::Unanswered => PROBE_WINDOW,
            Suspicion::Restarted => RESTART_PROBE_WINDOW,
        };
        // The peer's other connections (the ones it dialed to us, other
        // ALPNs), and what each had received when the probe began; and
        // the addresses this one talks to.
        let others = self.other_connections(id, stable);
        let addrs: Vec<iroh::TransportAddr> = conn
            .paths()
            .iter()
            .map(|p| p.remote_addr().clone())
            .collect();
        if self.alive(&conn, id, window).await {
            tracing::debug!(peer = %id.fmt_short(), ?why, "pooled connection answered its probe");
        } else if others.iter().any(|(c, rx, at)| {
            c.close_reason().is_none()
                && c.stats().udp_rx.datagrams > *rx
                && at.as_ref().is_some_and(|a| addrs.contains(a))
        }) {
            // This connection is dead, but the peer is not: another
            // connection to the same address (the same incarnation — a
            // restarted one listens on a new port) received datagrams
            // meanwhile. Drop this one only. Closing them all (the dead-
            // incarnation remedy below) cut a live lease holder's
            // backup appends mid-flight — the holder's own connection
            // to its backup was fine — and the backup sealed it
            // (slow-s3-no-seal: a forward stuck on a broken pooled
            // connection timed out, the probe found that connection
            // silent, and every connection to the holder was closed).
            tracing::info!(
                peer = %id.fmt_short(),
                ?why,
                window_ms = window.as_millis() as u64,
                "pooled P2P connection is dead, the peer is not (another connection to it is \
                 live); replacing only this one"
            );
            if let Some(conn) = self.unpool(&gate, stable).await {
                conn.close(iroh::endpoint::VarInt::from_u32(0), b"unresponsive");
            }
        } else {
            tracing::info!(
                peer = %id.fmt_short(),
                ?why,
                window_ms = window.as_millis() as u64,
                "pooled P2P connection is dead (peer restarted or gone); evicting it"
            );
            self.evict(id, stable, "unresponsive").await;
        }
    }

    /// Did `conn` show any sign of life within `window`? A `Ping` goes
    /// out on a fresh stream (its STREAM frame is ACK-eliciting); the
    /// answer is yes as soon as the connection receives any datagram at
    /// all, or the `Pong`.
    async fn alive(
        &self,
        conn: &iroh::endpoint::Connection,
        expect: iroh::EndpointId,
        window: Duration,
    ) -> bool {
        let received = || conn.stats().udp_rx.datagrams;
        let before = received();
        let deadline = tokio::time::Instant::now() + window;
        let ping = async {
            let (mut send, mut recv) = conn.open_bi().await?;
            let msg = Signed::new(&self.key, &Payload::Ping { node_id: 0 })?;
            crate::message::write_frame(&mut send, &msg).await?;
            send.finish().ok();
            let reply = crate::message::read_frame(&mut recv).await?;
            let (author, body) = reply.verify()?;
            anyhow::ensure!(
                author.as_bytes() == expect.as_bytes(),
                "pong from another key"
            );
            anyhow::ensure!(matches!(body, Payload::Pong { .. }), "not a pong");
            anyhow::Ok(())
        };
        tokio::pin!(ping);
        let mut ping_done = false;
        loop {
            if conn.close_reason().is_some() {
                return false;
            }
            if received() > before {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::select! {
                res = &mut ping, if !ping_done => {
                    if res.is_ok() {
                        return true;
                    }
                    // A failed ping alone proves nothing (the stream may
                    // have been refused); keep watching the counter.
                    ping_done = true;
                }
                _ = tokio::time::sleep(PROBE_TICK) => {}
            }
        }
    }

    /// Take `stable`'s connection out of `gate` if it is still the one
    /// pooled there.
    async fn unpool(
        &self,
        gate: &ConnectionSlot,
        stable: usize,
    ) -> Option<iroh::endpoint::Connection> {
        let mut slot = gate.lock().await;
        match slot.as_ref() {
            Some(conn) if conn.stable_id() == stable => slot.take(),
            _ => None,
        }
    }

    /// Every open connection to `id` other than `stable`'s, with its
    /// received-datagram count now and its selected path's address.
    fn other_connections(
        &self,
        id: iroh::EndpointId,
        stable: usize,
    ) -> Vec<(iroh::endpoint::Connection, u64, Option<iroh::TransportAddr>)> {
        let tracked = self.tracked.lock().unwrap();
        tracked
            .get(&id)
            .map(|list| {
                list.iter()
                    .filter_map(|weak| weak.upgrade())
                    .filter(|c| c.stable_id() != stable && c.close_reason().is_none())
                    .map(|c| {
                        let rx = c.stats().udp_rx.datagrams;
                        let at = c
                            .paths()
                            .iter()
                            .find(|p| p.is_selected())
                            .map(|p| p.remote_addr().clone());
                        (c, rx, at)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn track(&self, conn: &iroh::endpoint::Connection) {
        let mut tracked = self.tracked.lock().unwrap();
        let list = tracked.entry(conn.remote_id()).or_default();
        list.retain(|weak| {
            weak.upgrade()
                .is_some_and(|conn| conn.close_reason().is_none())
        });
        list.push(conn.weak_handle());
    }

    /// Plan 31 C8: close every pooled and tracked connection.
    async fn close_all(&self, why: &'static str) -> usize {
        let slots: Vec<ConnectionSlot> =
            self.connections.lock().unwrap().values().cloned().collect();
        let mut closed = 0usize;
        for slot in slots {
            if let Some(conn) = slot.lock().await.take() {
                if conn.close_reason().is_none() {
                    conn.close(iroh::endpoint::VarInt::from_u32(0), why.as_bytes());
                    closed += 1;
                }
            }
        }
        let tracked = std::mem::take(&mut *self.tracked.lock().unwrap());
        for conn in tracked
            .into_values()
            .flatten()
            .filter_map(|weak| weak.upgrade())
        {
            if conn.close_reason().is_none() {
                conn.close(iroh::endpoint::VarInt::from_u32(0), why.as_bytes());
                closed += 1;
            }
        }
        closed
    }

    /// Close every open connection, pooled or tracked, that has a direct
    /// path over one of the `lost` local addresses
    /// ([`crate::addrs::path_uses_lost`]), and every one for which that
    /// cannot be told (no path at all, or a direct one whose local
    /// address noq does not know); then run the evict hook for each peer
    /// that lost one, so it is re-dialed and its gossip link re-formed at
    /// once. A connection whose paths all run between addresses that are
    /// still there is left alone.
    ///
    /// Any open path counts, not only the selected one: each end selects
    /// its own path, and a connection that one end resets while the other
    /// keeps it is worse than either — the close frame may go out over the
    /// dead path, and the keeping end then waits on a connection the other
    /// end forgot (seen in `tests/addr_churn.rs`: a 23 s gap). Both ends
    /// see the same set of paths, so on a peer on this host — the case a
    /// lost address breaks — both reset the same connections. Returns
    /// `(closed, kept)`. See [`watch_local_addrs`].
    async fn reset_using(
        &self,
        lost: &HashSet<std::net::IpAddr>,
        why: &'static str,
    ) -> (usize, usize) {
        let affected = |conn: &iroh::endpoint::Connection| {
            let paths = conn.paths();
            let mut verdict = Some(false);
            for path in paths.iter() {
                // A relay path does not use a local interface address the
                // way a direct one does: the relay connection is TCP, and
                // its own reconnect handles a lost source address.
                let iroh::TransportAddr::Ip(remote) = path.remote_addr() else {
                    continue;
                };
                let local = match path.local_addr() {
                    iroh::endpoint::LocalTransportAddr::Ip(ip) => *ip,
                    _ => None,
                };
                match crate::addrs::path_uses_lost(Some(remote.ip()), local, lost) {
                    Some(true) => return true,
                    Some(false) => {}
                    None => verdict = None,
                }
            }
            // No path left at all, or one whose local end is unknown.
            paths.iter().next().is_none() || verdict.is_none()
        };
        let mut peers: HashSet<iroh::EndpointId> = HashSet::new();
        let (mut closed, mut kept) = (0usize, 0usize);
        let slots: Vec<(iroh::EndpointId, ConnectionSlot)> = self
            .connections
            .lock()
            .unwrap()
            .iter()
            .map(|(id, slot)| (*id, slot.clone()))
            .collect();
        for (id, slot) in slots {
            let mut slot = slot.lock().await;
            match slot.as_ref() {
                Some(conn) if conn.close_reason().is_none() && affected(conn) => {
                    if let Some(conn) = slot.take() {
                        conn.close(iroh::endpoint::VarInt::from_u32(0), why.as_bytes());
                    }
                    closed += 1;
                    peers.insert(id);
                }
                Some(conn) if conn.close_reason().is_none() => kept += 1,
                _ => {}
            }
        }
        let tracked: Vec<iroh::endpoint::Connection> = self
            .tracked
            .lock()
            .unwrap()
            .values()
            .flatten()
            .filter_map(|weak| weak.upgrade())
            .collect();
        for conn in tracked {
            if conn.close_reason().is_some() {
                continue;
            }
            if affected(&conn) {
                conn.close(iroh::endpoint::VarInt::from_u32(0), why.as_bytes());
                closed += 1;
                peers.insert(conn.remote_id());
            } else {
                kept += 1;
            }
        }
        let hook = self.on_evict.lock().unwrap().clone();
        if let Some(hook) = hook {
            for id in peers {
                hook(id);
            }
        }
        (closed, kept)
    }

    /// Drop `stable`'s connection from the pool (if it is still the one
    /// pooled for `id`) and close it, so requests still waiting on it
    /// fail now instead of at their own timeouts; then close every other
    /// connection to `id` (see [`Pool`] for why).
    async fn evict(&self, id: iroh::EndpointId, stable: usize, why: &'static str) {
        let Some(gate) = self.gate(&id) else {
            return;
        };
        let Some(conn) = self.unpool(&gate, stable).await else {
            return;
        };
        conn.close(iroh::endpoint::VarInt::from_u32(0), why.as_bytes());
        let others = self.tracked.lock().unwrap().remove(&id).unwrap_or_default();
        let mut closed = 0usize;
        for other in others.iter().filter_map(|weak| weak.upgrade()) {
            if other.close_reason().is_none() {
                other.close(
                    iroh::endpoint::VarInt::from_u32(0),
                    b"peer incarnation is dead; resetting",
                );
                closed += 1;
            }
        }
        tracing::debug!(peer = %id.fmt_short(), closed, "closed every connection to the peer");
        let hook = self.on_evict.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(id);
        }
    }
}

/// Resets the connections that used a local address on an admitted
/// interface ([`AddrPolicy`]) when it disappears — an interface went down
/// or away, or lost its address.
///
/// noq does migrate a path whose local address vanished, but when the
/// vanished address is also the *remote* one — a peer on this same host,
/// reached over a local address — it re-opens the path to that same dead
/// address, still marked validated and available, and keeps scheduling
/// stream data onto it until the path idles out; a stream that had data in
/// flight there (gossip's long-lived ones) could stay stuck for minutes. A
/// fresh connection lands on an address that exists within a round trip,
/// so on such a loss every connection whose selected path had the lost
/// address at either end — or whose path cannot be told — is closed
/// (peers get the close frame and drop theirs at once), and the evict
/// hook re-dials each such peer and re-forms gossip ([`Pool::reset_using`]).
/// Connections to other peers, over other addresses, carry on. Addresses
/// on refused interfaces (container bridges, which come and go all the
/// time) never trigger this: the link does not use them (see
/// [`crate::addrs`]).
///
/// `CONSTELLATION_P2P_TEST_NO_ADDR_WATCH=1` turns the watch off. It is a
/// test switch, not a knob: with it (and the old address and path idle
/// settings) `tests/addr_churn.rs` measures the behaviour before this
/// fix.
async fn watch_local_addrs(pool: Weak<Pool>, policy: AddrPolicy) {
    use iroh::Watcher;
    if std::env::var("CONSTELLATION_P2P_TEST_NO_ADDR_WATCH").is_ok_and(|v| v == "1") {
        tracing::warn!("CONSTELLATION_P2P_TEST_NO_ADDR_WATCH: local address losses are ignored");
        return;
    }
    // Our own monitor, not iroh's address watch: without relays iroh
    // refreshes its direct addresses only on its 20–25 s periodic run
    // after the first link change, not on each one.
    let monitor = match netwatch::netmon::Monitor::new().await {
        Ok(monitor) => monitor,
        Err(error) => {
            // Expected in a container without netlink access; the paths
            // still idle out, only slower.
            tracing::info!(%error, "no network monitor; a lost local address is noticed only \
                 when the paths using it idle out");
            return;
        }
    };
    let mut state = monitor.interface_state();
    let mut known = policy.admitted_local_ips(&crate::addrs::interfaces_of(&state.get()));
    while let Ok(now) = state.updated().await {
        let now = policy.admitted_local_ips(&crate::addrs::interfaces_of(&now));
        let lost: Vec<std::net::IpAddr> = known.difference(&now).copied().collect();
        known = now;
        if lost.is_empty() {
            continue;
        }
        let Some(pool) = pool.upgrade() else {
            return;
        };
        let lost: HashSet<std::net::IpAddr> = lost.into_iter().collect();
        let (closed, kept) = pool.reset_using(&lost, "local address gone").await;
        tracing::info!(
            ?lost,
            closed,
            kept,
            "a local P2P address disappeared; reset the peer connections that used it, to \
             re-dial over the remaining ones"
        );
    }
}

/// Watches every completed handshake: tracks the connection (for
/// [`Pool::evict`]), and on an inbound one — a peer dialing us afresh
/// may be a new incarnation behind a connection we still pool — has the
/// pooled connection probed.
///
/// An inbound handshake completes before any allowlist check: the QUIC
/// handshake proves only that the peer holds *some* key, and the
/// registry check happens later, in the ALPN handler. Tracking every
/// inbound connection here would let anyone who can reach the endpoint
/// grow [`Pool::tracked`] without bound — one entry per fresh random
/// key, each pinning its dead connection's allocation through the weak
/// handle — before ever being refused. So an inbound connection is
/// tracked and probed only when its key is already on the allowlist;
/// a peer that enrolled since the last registry read is admitted by the
/// handler's refresh as before, and its restart, if any, is caught by
/// the registry path (`Peers::refresh_registry` → `suspect_moved`: the
/// restart publishes a new port).
///
/// Plan 31 C8: it is also where [`Admission`] is enforced — an inbound
/// connection is refused right after its handshake while this endpoint is
/// dial-only or quiesced, and an outbound one before any packet is sent
/// while quiesced — so the refusal covers every ALPN (our requests and
/// gossip's) with no change to either protocol.
#[derive(Debug)]
struct InboundWatch {
    pool: Weak<Pool>,
    allow: Weak<Mutex<Allowlist>>,
    admission: Arc<Admission>,
}

/// Plan 31 C8: which connections this endpoint admits (`P2pMode::DialOnly`
/// refuses inbound ones; a host suspension refuses inbound ones and every
/// outbound dial but this node's own requests — gossip's). Plain atomics:
/// read once per handshake.
#[derive(Debug, Default)]
struct Admission {
    refuse_inbound: std::sync::atomic::AtomicBool,
    refuse_gossip: std::sync::atomic::AtomicBool,
}

/// The QUIC close code an inbound connection is refused with while this
/// endpoint does not accept (dial-only, or quiesced for a suspension).
const NOT_ACCEPTING: u32 = 0x4e41; // "NA"

impl iroh::endpoint::EndpointHooks for InboundWatch {
    fn before_connect<'a>(
        &'a self,
        _remote_addr: &'a EndpointAddr,
        alpn: &'a [u8],
    ) -> impl std::future::Future<Output = iroh::endpoint::BeforeConnectOutcome> + Send + 'a {
        let refuse = alpn != ALPN
            && self
                .admission
                .refuse_gossip
                .load(std::sync::atomic::Ordering::Relaxed);
        std::future::ready(if refuse {
            iroh::endpoint::BeforeConnectOutcome::Reject
        } else {
            iroh::endpoint::BeforeConnectOutcome::Accept
        })
    }

    fn after_handshake<'a>(
        &'a self,
        conn: &'a iroh::endpoint::Connection,
    ) -> impl std::future::Future<Output = iroh::endpoint::AfterHandshakeOutcome> + Send + 'a {
        let inbound = conn.side() == iroh::endpoint::Side::Server;
        if inbound
            && self
                .admission
                .refuse_inbound
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            let refused = iroh::endpoint::AfterHandshakeOutcome::Reject {
                error_code: iroh::endpoint::VarInt::from_u32(NOT_ACCEPTING),
                reason: b"not accepting connections (dial-only or suspended)".to_vec(),
            };
            return std::future::ready(refused);
        }
        if let Some(pool) = self.pool.upgrade() {
            // `contains`, not `check`: the handler's own `check` must be
            // the one that arms the miss-refresh cooldown.
            let known = !inbound
                || self.allow.upgrade().is_some_and(|allow| {
                    allow
                        .lock()
                        .unwrap()
                        .contains(&crate::identity::hex32(conn.remote_id().as_bytes()))
                });
            if known {
                pool.track(conn);
                if inbound {
                    pool.suspect(conn.remote_id(), Suspicion::Restarted);
                }
            }
        }
        std::future::ready(iroh::endpoint::AfterHandshakeOutcome::accept())
    }
}

/// Probes the pooled connection if the request it guards is dropped
/// before it completes — the only trace a caller's timeout leaves.
struct InFlight {
    pool: Arc<Pool>,
    id: iroh::EndpointId,
    done: bool,
}

impl InFlight {
    fn new(pool: &Arc<Pool>, id: iroh::EndpointId) -> Self {
        Self {
            pool: pool.clone(),
            id,
            done: false,
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if !self.done {
            self.pool.suspect(self.id, Suspicion::Unanswered);
        }
    }
}

/// One hash-prefix bucket of a peer's cache bloom, as gossiped.
pub struct DigestSnapshot {
    pub node_id: u64,
    pub generation: u64,
    pub bits: Vec<u8>,
    pub nbits: u64,
    pub k: u32,
    pub n: u64,
    pub bucket: u32,
    pub buckets: u32,
}

/// Add-only bloom delta. `adds` are raw hashes; the receiver routes
/// each one with this sender's `buckets`.
pub struct DigestDelta {
    pub node_id: u64,
    pub generation: u64,
    pub adds: Vec<[u8; 32]>,
    pub buckets: u32,
}

/// One chunk pulled from a peer, with the timing the source selector
/// needs. `ttfb` is measured to the control reply, so `ttfb` and the
/// caller's end-to-end duration bracket the body transfer.
#[derive(Debug)]
pub struct ChunkFetch {
    pub data: Vec<u8>,
    pub ttfb: std::time::Duration,
    pub rtt: Option<std::time::Duration>,
    pub path: PathKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    Direct,
    Relay,
    Unknown,
}

impl PathKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Relay => "relay",
            Self::Unknown => "unknown",
        }
    }
}

/// What the daemon gives the endpoint so it can serve peer requests.
/// Kept as a trait object so `cli` owns the shipper/lease logic and this
/// crate stays free of filesystem concerns.
///
/// `lease_requested` is async because answering it means flushing to S3;
/// doing that on a blocking call inside the accept task would stall a
/// runtime worker.
pub trait PeerService: Send + Sync + 'static {
    /// A peer published a segment: tail now instead of at the next poll
    /// (unless this node follows the holder's log stream, which carries
    /// it). Plan 30 §M7: a hint, never the segment itself.
    fn segment_published(&self, part: &str, seq: u64, epoch: u64);
    /// Plan 30 §M7: `requester` subscribes to this node's log stream from
    /// sequence `from`. The returned receiver yields what to write on the
    /// stream, in order; the stream ends when it closes (or after an
    /// [`LogEvent::End`]). `None` refuses at once (no log to serve).
    fn log_subscribe(
        &self,
        _requester: u64,
        _req_id: u64,
        _from: u64,
    ) -> Option<tokio::sync::mpsc::Receiver<LogEvent>> {
        None
    }
    /// A peer wants `part`'s lease. Returns the reply to send.
    /// `epoch_applied`: see `Payload::LeaseRequest`.
    fn lease_requested(
        &self,
        part: String,
        requester: u64,
        epoch_applied: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>>;
    /// A peer proposed a continuation epoch. Persist the promise, then
    /// reply with an ack. Default declines (P2P-disabled / tests).
    fn epoch_proposed(
        &self,
        _epoch_id: String,
        _members: Vec<u64>,
        _base: Vec<(String, u64)>,
        _proposer: u64,
        _epoch_slack: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::EpochAck {
                epoch_id: String::new(),
                member: 0,
                accepted: false,
                claim: None,
                known: 0,
            }
        })
    }
    /// A peer redistributed activation. Default is a no-op.
    fn epoch_activated(&self, _activation: crate::EpochActivation) {}
    /// Plan 30 §M10: a proposer abandoned `epoch_id`. Default no-op.
    fn epoch_aborted(&self, _epoch_id: String, _proposer: u64) {}
    /// Plan 30 §M10: a would-be taker asks for a heartbeat promise.
    /// Default refuses (P2P-disabled / tests).
    fn promise_requested(
        &self,
        _requester: u64,
        req_id: u64,
        _expires_unix_ms: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::PromiseReply {
                req_id,
                until: None,
                epoch_slack: 0,
            }
        })
    }
    /// Cooperative-cache digest snapshot from a peer.
    fn cache_digest(&self, _digest: DigestSnapshot) {}
    fn cache_digest_delta(&self, _delta: DigestDelta) {}
    /// Plan 30 §M15: a peer's exact-membership heartbeat.
    fn cache_summary(&self, _node_id: u64, _summary: crate::reconcile::Summary) {}
    /// Plan 30 §M15: a peer's exact adds/removes for one publish tick.
    fn cache_set_delta(&self, _node_id: u64, _delta: crate::reconcile::Delta) {}
    /// Plan 30 §M15: answer one reconciliation round against this node's
    /// published chunk set. The default declines (`processed: 0`), which
    /// the initiator treats as "no progress" and abandons.
    fn reconcile_requested(
        &self,
        _queries: Vec<crate::reconcile::Query>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::ReconcileReply {
                reply: crate::reconcile::Reply {
                    summary: crate::reconcile::Summary {
                        incarnation: 0,
                        seq: 0,
                        root: [0; 16],
                        count: 0,
                    },
                    processed: 0,
                    answers: Vec::new(),
                },
            }
        })
    }
    /// Serve a clean/pinned chunk, or decline with the reason (busy,
    /// absent, or recently removed — see [`ChunkDecline`]). `from_hex`
    /// is the requester's node-key hex, used for the per-peer
    /// concurrency cap.
    ///
    /// Async for the same reason as `lease_requested`: answering means
    /// reading and verifying up to a whole chunk from disk, which must
    /// not happen inline on a runtime worker.
    fn serve_chunk(
        &self,
        _hash: [u8; 32],
        _from_hex: String,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, ChunkDecline>> + Send + '_>,
    > {
        Box::pin(async move { Err(ChunkDecline::Busy) })
    }
    /// EC2 finding 1: `requester` (which cannot reach S3) hands us chunks
    /// to fetch from it and upload. Default refuses.
    fn chunk_handoff_requested(
        &self,
        _requester: u64,
        req_id: u64,
        _hashes: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::ChunkHandoffReply {
                req_id,
                uploaded: false,
            }
        })
    }
    /// Non-holder asked us to journal `op`. Default declines with an
    /// empty outcome; callers treat that as `MutateOutcome::Busy`.
    #[allow(clippy::too_many_arguments)]
    fn mutate_requested(
        &self,
        _part: String,
        _requester: u64,
        req_id: u64,
        _epoch_seen: u64,
        _op: Vec<u8>,
        _rid: (u64, u32, u64),
        _acked_through: u64,
        _deps: Vec<u8>,
        _pending: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::MutateReply {
                req_id,
                outcome: Vec::new(),
                base: None,
                position_seq: 0,
                position_pending: None,
                position_streams: Vec::new(),
                gen: 0,
            }
        })
    }
    /// Plan 32 Step 0.1: `requester` asks this node, as the root-lease
    /// holder, to execute a snapshot batch. The sender is already checked
    /// to be `requester` (the batch's rid keys this node's dedup). Default:
    /// not the holder (a node without a snapshot executor).
    fn snapshot_batch_requested(
        &self,
        _requester: u64,
        req_id: u64,
        _rid: (u64, u32, u64),
        _items: Vec<crate::message::SnapshotItem>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::SnapshotBatchReply {
                req_id,
                outcome: crate::message::SnapshotBatchOutcome::NotHolder,
            }
        })
    }
    /// `from` reports chunks it forwarded as pending now durable in S3
    /// ([`Payload::ChunksDurable`]). Default: ignored.
    fn chunks_durable(
        &self,
        _from: u64,
        _hashes: Vec<[u8; 32]>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {})
    }
    /// Plan 30 §M11: a delegate's stream batch for the root. Default:
    /// refused.
    fn delegate_stream_requested(
        &self,
        _from: u64,
        req_id: u64,
        gen: u64,
        _txs: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::DelegateStreamAck {
                req_id,
                gen,
                through: 0,
                refused: true,
            }
        })
    }
    /// Plan 30 §M11 phase 2b: a delegate's append to this backup.
    /// Default: sealed (nothing held).
    fn deleg_backup_append_requested(
        &self,
        _from: u64,
        req_id: u64,
        gen: u64,
        _txs: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::DelegBackupAck {
                req_id,
                gen,
                acked: 0,
                sealed: true,
            }
        })
    }
    /// Plan 30 §M11 phase 2b: the root's seal request. Default: not a
    /// backup of it.
    fn deleg_seal_requested(
        &self,
        _root: u64,
        req_id: u64,
        gen: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::DelegSealed {
                req_id,
                gen,
                sealed: false,
                txs: Vec::new(),
            }
        })
    }
    /// Plan 30 §M11: a delegate's renewal. Default: refused (ttl 0).
    fn deleg_renew_requested(
        &self,
        _from: u64,
        req_id: u64,
        gen: u64,
        _backup: u64,
        _stream_head: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::DelegRenewed {
                req_id,
                gen,
                ttl_ms: 0,
                locks: Vec::new(),
                lock_grace_ms: 0,
                lock_floor: Vec::new(),
            }
        })
    }
    /// Plan 30 §M11: the root's recall. Default: nothing executed.
    fn deleg_recall_requested(
        &self,
        _root: u64,
        req_id: u64,
        _dir: u64,
        gen: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::DelegRecalled {
                req_id,
                gen,
                through: 0,
                locks: Vec::new(),
                lock_floor: Vec::new(),
            }
        })
    }
    /// Plan 30 §M8: a strict reader's ReadIndex. Default: not the holder.
    fn read_index_requested(
        &self,
        _requester: u64,
        req_id: u64,
        _ino: u64,
        _dir: bool,
        _name: Option<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::ReadIndexReply {
                req_id,
                status: 1,
                holder: 0,
                position_seq: 0,
                position_pending: None,
                grant: None,
                position_streams: Vec::new(),
            }
        })
    }
    /// Plan 30 §M8: the sequencer recalls a read delegation. The default
    /// holds none, so it acks at once.
    fn read_recall_requested(
        &self,
        _holder: u64,
        req_id: u64,
        _ino: u64,
        _grant: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move { Payload::ReadRecalled { req_id } })
    }
    /// Plan 30 §M9: the holder streams journal transactions to this node
    /// as its backup. Default: this node backs nobody (`sealed`, so the
    /// holder never counts it).
    #[allow(clippy::too_many_arguments)]
    fn backup_append_requested(
        &self,
        _holder: u64,
        req_id: u64,
        epoch: u64,
        _config_version: u64,
        _from: u64,
        _txs: Vec<u8>,
        _through: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::BackupAck {
                req_id,
                epoch,
                acked: 0,
                sealed: true,
            }
        })
    }
    /// Plan 30 §M9: backup-acked transactions streamed ahead of S3.
    /// Default ignores them.
    fn stream_ahead(&self, _from: u64, _epoch: u64, _base: u64, _txs: Vec<u8>) {}
    /// Plan 30 §M14: a node asks this one, as the owning sequencer, for
    /// a lock grant. Default: busy (no lock service here).
    fn lock_requested(
        &self,
        _requester: u64,
        req_id: u64,
        _ino: u64,
        _exclusive: bool,
        _blocking: bool,
        _sent: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::LockReply {
                req_id,
                outcome: crate::message::LockOutcomeWire::Busy,
            }
        })
    }
    /// Plan 30 §M14: the owner recalls a grant this node holds. Default:
    /// holds none, acks at once.
    fn lock_recall_requested(
        &self,
        _owner: u64,
        req_id: u64,
        _ino: u64,
        _grant: (u64, u64),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move { Payload::LockRecalled { req_id } })
    }
    /// Plan 30 §M14: a holder renews its grants here. Default: not the
    /// owner of any of them.
    fn lock_renew_requested(
        &self,
        _from: u64,
        req_id: u64,
        entries: Vec<crate::message::LockRenewWire>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::LockRenewed {
                req_id,
                results: entries
                    .into_iter()
                    .map(|e| {
                        (
                            e.ino,
                            e.grant,
                            crate::message::LockRenewResultWire::NotOwner { owner: 0 },
                        )
                    })
                    .collect(),
            }
        })
    }
    /// Plan 30 §M14: `getlk` against this node as the owner. Default: not
    /// the owner.
    fn lock_test_requested(
        &self,
        _requester: u64,
        req_id: u64,
        _ino: u64,
        _exclusive: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::LockTestReply {
                req_id,
                outcome: crate::message::LockTestOutcomeWire::NotOwner { owner: 0 },
            }
        })
    }
    /// Plan 30 §M14, one way: a parked request's grant, pushed.
    fn lock_granted(
        &self,
        _from: u64,
        _ino: u64,
        _sent: i64,
        _outcome: crate::message::LockOutcomeWire,
    ) {
    }
    /// Plan 30 §M14, one way: a holder released a grant.
    fn lock_released(&self, _from: u64, _ino: u64, _grant: (u64, u64), _position: &[u8]) {}
    /// Plan 30 §M14, one way: the holder's grant table, for a backup.
    fn lock_mirror(&self, _from: u64, _ver: u64, _grants: Vec<u8>, _floor: Vec<u8>) {}
    /// Holder offered us this lease (placement). Default ignores it.
    fn lease_offered(&self, _part: String, _epoch: u64) {}
    /// A peer gossiped its RTT vector. Default ignores it.
    fn peer_rtts(&self, _node_id: u64, _rtts: Vec<(u64, u16)>) {}
    /// This node's id, for `Ping`/`Pong`.
    fn node_id(&self) -> u64;
    /// EC2 follow-up 3c: probe this node's own S3 path now, for a
    /// `PingS3`'s `Pong::s3_ok`. Default: unknown (`false`).
    fn s3_probe(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async { false })
    }
}

/// A live P2P endpoint.
/// Plan 30 §M7: one event on a log stream, as the holder writes it and
/// the subscriber reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEvent {
    /// Frame `n`: a segment (`(seq, S3 object bytes)`) or a heartbeat.
    Frame {
        n: u64,
        epoch: u64,
        head: u64,
        segment: Option<(u64, Vec<u8>)>,
    },
    /// The holder ended the subscription (`refused`: it never held).
    End { refused: bool },
}

/// How long a holder waits for one log-stream frame to be taken by a
/// subscriber before giving that subscriber up (it falls back to S3).
pub const LOG_FRAME_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub struct P2p {
    endpoint: Endpoint,
    gossip: Gossip,
    topic: TopicId,
    key: SecretKey,
    allow: Arc<Mutex<Allowlist>>,
    /// Registry-sourced peer addresses, fed to iroh so gossip (which
    /// bootstraps from bare endpoint ids) can actually dial them. With
    /// address publishing disabled this is the only address source.
    lookup: MemoryLookup,
    /// Broadcast handle for the joined topic, once it exists.
    sender: Arc<tokio::sync::Mutex<Option<iroh_gossip::api::GossipSender>>>,
    /// One gate per remote prevents dial storms while allowing unrelated
    /// peers to connect concurrently. QUIC streams multiplex over the
    /// retained connection. See [`Pool`] for how dead ones are evicted.
    pool: Arc<Pool>,
    /// Active relay policy label (`disabled` / `default` / URL…).
    relay: String,
    /// Plan 31 C8: dial-only and quiesced ([`InboundWatch`]).
    admission: Arc<Admission>,
    /// The path selector, fed each peer's published addresses
    /// ([`crate::addrs`]).
    preferred: Arc<PreferredPaths>,
    /// Which local interfaces' addresses [`Self::advertised_addr`] keeps.
    policy: AddrPolicy,
    /// `CONSTELLATION_P2P_DIAL_TIMEOUT_MS`, read at spawn.
    dial_timeout: Duration,
}

/// Derive the gossip topic. Prefers the `gossip_secret` from
/// `meta.json` so topic membership is not guessable from a bucket name;
/// falls back to hashing the filesystem UUID for filesystems created
/// before the secret existed (weaker: anyone who learns the UUID can
/// join the topic, but messages are still signed and the allowlist still
/// gates direct connections, so the worst case is unsolicited traffic).
pub fn topic_for(gossip_secret: Option<&[u8; 32]>, fs_uuid: &str) -> TopicId {
    match gossip_secret {
        Some(s) => TopicId::from_bytes(*s),
        None => TopicId::from_bytes(*blake3::hash(fs_uuid.as_bytes()).as_bytes()),
    }
}

impl P2p {
    /// Bind the endpoint and spawn gossip. Errors are the caller's cue to
    /// run without a fast path.
    ///
    /// Relay behaviour comes from [`RelayPolicy::from_env`] unless
    /// `relay` is passed explicitly (tests).
    pub async fn spawn(key: SecretKey, topic: TopicId) -> Result<Self> {
        Self::spawn_with(key, topic, RelayPolicy::from_env()?).await
    }

    pub async fn spawn_with(key: SecretKey, topic: TopicId, relay: RelayPolicy) -> Result<Self> {
        let lookup = MemoryLookup::new();
        let pool = Arc::new(Pool::new(key.clone()));
        let allow = Arc::new(Mutex::new(Allowlist::new()));
        let relay_mode = relay.to_iroh()?;
        let relay_label = relay.label();
        let admission = Arc::new(Admission::default());
        let preferred = Arc::new(PreferredPaths::default());
        let endpoint = Endpoint::builder(presets::Minimal)
            // Registry remains the peer directory (DESIGN.md §8). Relays
            // are optional connectivity help when direct addrs cannot
            // reach (NAT / no shared L3). See docs/reference/features/p2p-relays.md.
            .relay_mode(relay_mode)
            .secret_key(key.clone())
            .address_lookup(lookup.clone())
            .alpns(vec![ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
            .transport_config(transport_config(path_idle_timeout()))
            .path_selector(preferred.clone())
            .hooks(InboundWatch {
                pool: Arc::downgrade(&pool),
                allow: Arc::downgrade(&allow),
                admission: admission.clone(),
            })
            .bind()
            .await
            .context("binding the iroh endpoint")?;
        let policy = AddrPolicy::from_env();
        tokio::spawn(watch_local_addrs(Arc::downgrade(&pool), policy.clone()));
        let gossip = Gossip::builder()
            .max_message_size(crate::message::GOSSIP_MAX_MESSAGE_SIZE)
            .spawn(endpoint.clone());
        Ok(Self {
            endpoint,
            gossip,
            topic,
            key,
            allow,
            lookup,
            sender: Arc::new(tokio::sync::Mutex::new(None)),
            pool,
            relay: relay_label,
            admission,
            preferred,
            policy,
            dial_timeout: dial_timeout(),
        })
    }

    /// Plan 31 C8: refuse (or accept again) every inbound connection, of
    /// any ALPN, right after its handshake — `P2pMode::DialOnly`, and a
    /// suspension. Connections already open are not touched (see
    /// [`Self::close_all`]).
    pub fn set_accept_inbound(&self, accept: bool) {
        self.admission
            .refuse_inbound
            .store(!accept, std::sync::atomic::Ordering::Relaxed);
    }

    /// Plan 31 C8: refuse (or allow again) every outbound dial except
    /// this node's own requests (our ALPN) — gossip's re-dials, while
    /// suspended: the node leaves the mesh, but an op that still arrives
    /// can forward to the holder directly.
    pub fn set_gossip(&self, allow: bool) {
        self.admission
            .refuse_gossip
            .store(!allow, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether inbound connections are admitted.
    pub fn accepts_inbound(&self) -> bool {
        !self
            .admission
            .refuse_inbound
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether gossip may dial out.
    pub fn gossips(&self) -> bool {
        !self
            .admission
            .refuse_gossip
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Plan 31 C8: close every connection this endpoint has, either side,
    /// any ALPN, with a close frame, so each peer learns at once rather
    /// than at its idle timeout (a peer's pooled request fails now and
    /// its routing moves on). Returns how many were open.
    pub async fn close_all(&self, why: &'static str) -> usize {
        self.pool.close_all(why).await
    }

    /// Plan 31 C8 / plan 36 settled decision 20: the host says the
    /// network changed (an interface, a route, Wi-Fi to cellular); iroh
    /// re-probes its paths now instead of when it next notices.
    pub async fn network_change(&self) {
        self.endpoint.network_change().await;
    }

    /// Teach iroh how to reach a peer learned from the registry, and
    /// prefer paths to those addresses ([`crate::addrs`]).
    pub fn learn_addr(&self, addr: EndpointAddr) {
        self.preferred.learn(&addr);
        self.lookup.add_endpoint_info(addr);
    }

    /// Every address iroh knows for this endpoint, container bridges
    /// included. What peers should dial is [`Self::advertised_addr`].
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// The address to publish in the registry: [`Self::addr`] without the
    /// addresses of interfaces the [`AddrPolicy`] refuses. Waits up to
    /// [`ADDR_SCAN_WAIT`] for iroh's first interface scan, so a node that
    /// mounts right after binding does not publish an empty address.
    pub async fn advertised_addr(&self) -> EndpointAddr {
        let deadline = tokio::time::Instant::now() + ADDR_SCAN_WAIT;
        while self.endpoint.addr().addrs.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.advertised_addr_now().await
    }

    /// [`Self::advertised_addr`] without the wait: for a periodic check
    /// that must not stall while the node has no address at all.
    pub async fn advertised_addr_now(&self) -> EndpointAddr {
        self.policy.filter_now(&self.endpoint.addr()).await
    }

    /// Relay policy label active on this endpoint.
    pub fn relay_label(&self) -> &str {
        &self.relay
    }

    pub fn pubkey_hex(&self) -> String {
        crate::identity::pubkey_hex(&self.key.public())
    }

    /// Update the accept-time allowlist from the registry.
    pub fn set_allowed(&self, keys: impl IntoIterator<Item = String>) {
        self.allow.lock().unwrap().replace(keys);
    }

    pub fn allowed_len(&self) -> usize {
        self.allow.lock().unwrap().len()
    }

    /// Is `pubkey_hex` currently permitted? `Refresh` is reported as not
    /// allowed; the caller refreshes and asks again.
    pub fn check(&self, pubkey_hex: &str) -> Decision {
        self.allow.lock().unwrap().check(pubkey_hex)
    }

    /// Join the gossip topic, bootstrapping from `peers`, and return the
    /// receiver so the caller can drive incoming events. The sender is
    /// retained for [`P2p::broadcast`].
    pub async fn join(
        &self,
        peers: Vec<iroh::EndpointId>,
    ) -> Result<iroh_gossip::api::GossipReceiver> {
        let topic = self.gossip.subscribe(self.topic, peers).await?;
        let (tx, rx) = topic.split();
        *self.sender.lock().await = Some(tx);
        Ok(rx)
    }

    /// Broadcast a signed payload to the topic. Best effort: a failure
    /// only means peers learn from their next poll instead.
    ///
    /// Gossip delivers whole datagrams, so this sends bare postcard —
    /// the 4-byte length prefix from [`Signed::encode`] exists only to
    /// frame messages on a byte stream, and including it here would make
    /// every receiver's decode fail silently.
    pub async fn broadcast(&self, payload: &Payload) -> Result<()> {
        let msg = Signed::new(&self.key, payload)?;
        let body = msg.encode_bare()?;
        anyhow::ensure!(
            body.len() <= crate::message::GOSSIP_CONTENT_LIMIT,
            "gossip content is {} bytes, limit is {}",
            body.len(),
            crate::message::GOSSIP_CONTENT_LIMIT
        );
        let guard = self.sender.lock().await;
        let Some(tx) = guard.as_ref() else {
            anyhow::bail!("gossip topic not joined yet");
        };
        tx.broadcast(body.into()).await?;
        Ok(())
    }

    /// Ask `peer` to (re)join our gossip topic neighborhood — a `Join`,
    /// as at bootstrap. Used after a peer's dead incarnation was evicted
    /// and every connection to it closed: that drops it from the active
    /// view, and neither side would otherwise re-form the link (the new
    /// incarnation's own `Join` may already have been swallowed while
    /// the stale neighbor state still stood).
    pub async fn rejoin(&self, peer: iroh::EndpointId) -> Result<()> {
        let guard = self.sender.lock().await;
        let Some(tx) = guard.as_ref() else {
            anyhow::bail!("gossip topic not joined yet");
        };
        tx.join_peers(vec![peer]).await?;
        Ok(())
    }

    /// Send `payload` to one peer and wait for a single reply.
    ///
    /// `peer` is the full [`EndpointAddr`] from the registry, not just a
    /// key: with address publishing disabled the registry record is the
    /// only way to learn how to dial.
    pub async fn request(&self, peer: EndpointAddr, payload: &Payload) -> Result<Payload> {
        let expect = peer.id;
        let conn = self.connection(&peer).await?;
        let mut in_flight = InFlight::new(&self.pool, expect);
        let result = async {
            let (mut send, mut recv) = conn.open_bi().await.context("opening a stream")?;
            let msg = Signed::new(&self.key, payload)?;
            crate::message::write_frame(&mut send, &msg).await?;
            send.finish().ok();
            let reply = crate::message::read_frame(&mut recv).await?;
            let (author, body) = reply.verify()?;
            anyhow::ensure!(
                author.as_bytes() == expect.as_bytes(),
                "reply signed by an unexpected key"
            );
            Ok(body)
        }
        .await;
        in_flight.done = true;
        if result.is_err() {
            self.invalidate_connection(expect, conn.stable_id()).await;
        }
        result
    }

    /// Fetch one chunk from `peer`. The control frames use the same
    /// signed postcard as everything else; the payload (up to the FS chunk
    /// size) follows as `u64be length + bytes` so we never shove 4 MiB
    /// through [`crate::message::MAX_FRAME`]. `Ok(Err(reason))` is a
    /// clean decline, distinct from a transport failure.
    pub async fn request_chunk(
        &self,
        peer: EndpointAddr,
        hash: &[u8; 32],
    ) -> Result<std::result::Result<ChunkFetch, ChunkDecline>> {
        let expect = peer.id;
        let first = self.connection(&peer).await?;
        let mut in_flight = InFlight::new(&self.pool, expect);
        let result = match self.request_chunk_on(&first, expect, hash).await {
            Ok(value) => Ok(value),
            Err(first_error) => {
                self.invalidate_connection(expect, first.stable_id()).await;
                let retry = self.connection(&peer).await?;
                self.request_chunk_on(&retry, expect, hash)
                    .await
                    .with_context(|| format!("chunk stream failed after redial: {first_error:#}"))
            }
        };
        in_flight.done = true;
        result
    }

    /// Install the callback run after a dead pooled connection is
    /// evicted (the peer directory marks the peer down and re-dials).
    pub fn set_evict_hook(&self, hook: EvictHook) {
        *self.pool.on_evict.lock().unwrap() = Some(hook);
    }

    /// The peer `id` may have restarted (its registry record changed):
    /// probe the connection pooled for it and evict it if it is dead.
    pub fn suspect_restart(&self, id: iroh::EndpointId) {
        self.pool.suspect(id, Suspicion::Restarted);
    }

    /// The peer `id` re-published a record whose direct addresses are
    /// not the ones it published before (`published` are the new ones,
    /// [`crate::addrs::direct_addrs`]): probe the pooled connection as
    /// [`Self::suspect_restart`] does, unless its selected path goes to
    /// one of `published` — that address is still the peer's, so the
    /// connection is fine whatever else moved. A restart publishes a new
    /// port (iroh binds port 0), so it is never mistaken for that.
    pub fn suspect_moved(
        &self,
        id: iroh::EndpointId,
        published: &std::collections::BTreeSet<std::net::SocketAddr>,
    ) {
        let selected = self.path_summary_now(id).and_then(|s| s.selected_addr);
        if selected.is_some_and(|a| published.contains(&crate::addrs::canonical(a))) {
            return;
        }
        self.pool.suspect(id, Suspicion::Restarted);
    }

    /// Plan 30 §M7: open a log stream to `peer` (the holder) and return
    /// the events read from it. The subscription frame goes out on a new
    /// bidirectional stream of the pooled connection; a reader task then
    /// verifies each frame's author, reads the segment bytes that follow
    /// it and checks their hash, and forwards them in order. The channel
    /// closes when the stream ends or breaks — the caller's cue to fall
    /// back to S3 — and dropping the receiver ends the stream from this
    /// side.
    pub async fn open_log_stream(
        &self,
        peer: EndpointAddr,
        payload: &Payload,
    ) -> Result<tokio::sync::mpsc::Receiver<LogEvent>> {
        let expect = peer.id;
        let conn = self.connection(&peer).await?;
        let opened = async {
            let (mut send, recv) = conn.open_bi().await.context("opening a log stream")?;
            let msg = Signed::new(&self.key, payload)?;
            crate::message::write_frame(&mut send, &msg).await?;
            send.finish().ok();
            anyhow::Ok(recv)
        }
        .await;
        let mut recv = match opened {
            Ok(recv) => recv,
            Err(e) => {
                self.invalidate_connection(expect, conn.stable_id()).await;
                return Err(e);
            }
        };
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                let frame = match crate::message::read_frame(&mut recv).await {
                    Ok(frame) => frame,
                    Err(_) => return,
                };
                let Ok((author, body)) = frame.verify() else {
                    return;
                };
                if author.as_bytes() != expect.as_bytes() {
                    tracing::warn!("log stream frame signed by an unexpected key; closing");
                    return;
                }
                let event = match body {
                    Payload::LogFrame {
                        n,
                        epoch,
                        head,
                        segment,
                        ..
                    } => {
                        let segment = match segment {
                            None => None,
                            Some((seq, len, hash)) => {
                                if len > crate::message::MAX_LOG_SEGMENT {
                                    return;
                                }
                                let mut bytes = vec![0u8; len as usize];
                                if recv.read_exact(&mut bytes).await.is_err() {
                                    return;
                                }
                                if *blake3::hash(&bytes).as_bytes() != hash {
                                    tracing::warn!(
                                        seq,
                                        "log stream segment hash mismatch; closing"
                                    );
                                    return;
                                }
                                Some((seq, bytes))
                            }
                        };
                        LogEvent::Frame {
                            n,
                            epoch,
                            head,
                            segment,
                        }
                    }
                    Payload::LogEnd { refused, .. } => {
                        let _ = tx.send(LogEvent::End { refused }).await;
                        return;
                    }
                    _ => return,
                };
                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }

    /// Plan 30 §M4: every open path of the pooled connection to `id`,
    /// without waiting (`None` if none is pooled, or one is being dialed
    /// right now). For `status`, which must not block.
    pub fn path_summary_now(&self, id: iroh::EndpointId) -> Option<crate::paths::PathSummary> {
        let gate = self.pool.gate(&id)?;
        let slot = gate.try_lock().ok()?;
        slot.as_ref().map(crate::paths::PathSummary::of)
    }

    /// Whether a pooled, still-open QUIC connection to `id` exists right
    /// now. A request that timed out at the application level leaves
    /// the connection in the pool as long as the peer's transport still
    /// answers (see [`Pool`]: the timeout only triggers a probe, which
    /// evicts nothing that receives so much as an ACK), so this
    /// distinguishes "slow to answer" from "cannot be reached": a dial
    /// failure never pools anything, a transport error evicts it, a
    /// peer that closed it closed it, and a peer that died or restarted
    /// fails the probe. Plan 30 §M13 uses it to start a P2P outage only
    /// from the latter.
    pub async fn connection_alive(&self, id: iroh::EndpointId) -> bool {
        let gate = self.pool.gate(&id);
        let Some(gate) = gate else {
            return false;
        };
        let slot = gate.lock().await;
        slot.as_ref()
            .is_some_and(|conn| conn.close_reason().is_none())
    }

    /// Selected-path kind for a pooled connection, if any.
    pub async fn path_kind(&self, id: iroh::EndpointId) -> PathKind {
        let gate = self.pool.gate(&id);
        let Some(gate) = gate else {
            return PathKind::Unknown;
        };
        let slot = gate.lock().await;
        match slot.as_ref() {
            Some(conn) => transport_observation(conn).1,
            None => PathKind::Unknown,
        }
    }

    async fn request_chunk_on(
        &self,
        conn: &iroh::endpoint::Connection,
        expect: iroh::EndpointId,
        hash: &[u8; 32],
    ) -> Result<std::result::Result<ChunkFetch, ChunkDecline>> {
        use tokio::io::AsyncReadExt;
        let started = std::time::Instant::now();
        let (mut send, mut recv) = conn.open_bi().await.context("opening a chunk stream")?;
        let msg = Signed::new(&self.key, &Payload::ChunkRequest { hash: *hash })?;
        crate::message::write_frame(&mut send, &msg).await?;
        send.finish().ok();
        let reply = crate::message::read_frame(&mut recv).await?;
        // The control reply precedes the body, so this is a true
        // first-byte mark: everything after it is transfer time.
        let ttfb = started.elapsed();
        let (author, body) = reply.verify()?;
        anyhow::ensure!(
            author.as_bytes() == expect.as_bytes(),
            "chunk reply signed by an unexpected key"
        );
        match body {
            Payload::ChunkResponse {
                status: ChunkStatus::Declined(why),
                ..
            } => Ok(Err(why)),
            Payload::ChunkResponse {
                status: ChunkStatus::Found,
                ..
            } => {
                let len = recv.read_u64().await.context("chunk length")?;
                anyhow::ensure!(
                    len > 0 && len <= 64 * 1024 * 1024,
                    "implausible chunk length {len}"
                );
                let mut data = vec![0u8; len as usize];
                recv.read_exact(&mut data).await?;
                let (rtt, path) = transport_observation(conn);
                Ok(Ok(ChunkFetch {
                    data,
                    ttfb,
                    rtt,
                    path,
                }))
            }
            other => anyhow::bail!("unexpected chunk reply {other:?}"),
        }
    }

    async fn connection(&self, peer: &EndpointAddr) -> Result<iroh::endpoint::Connection> {
        let gate = self
            .pool
            .connections
            .lock()
            .unwrap()
            .entry(peer.id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone();
        let mut slot = gate.lock().await;
        if let Some(conn) = slot.as_ref() {
            // Not `weak_handle().upgrade()`: the pool's own strong handle
            // makes that always succeed, so a connection closed by the
            // peer or the idle timeout used to be handed out again.
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
            *slot = None;
        }
        // Bounded: the gate is held across the dial, and a dial whose
        // every address is gone would otherwise hold it — and every other
        // request to this peer — for the whole QUIC handshake timeout.
        let limit = self.dial_timeout;
        let conn = tokio::time::timeout(limit, self.endpoint.connect(peer.clone(), ALPN))
            .await
            .map_err(|_| anyhow::anyhow!("dialing peer timed out after {limit:?}"))?
            .context("dialing peer")?;
        *slot = Some(conn.clone());
        Ok(conn)
    }

    async fn invalidate_connection(&self, peer: iroh::EndpointId, stable_id: usize) {
        let gate = self.pool.gate(&peer);
        if let Some(gate) = gate {
            let mut slot = gate.lock().await;
            if slot
                .as_ref()
                .is_some_and(|conn| conn.stable_id() == stable_id)
            {
                *slot = None;
            }
        }
    }

    /// How many probes of a pooled connection were started.
    #[cfg(test)]
    pub(crate) fn probes_started(&self) -> usize {
        self.pool.probes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many remote endpoint ids have a tracked connection.
    #[cfg(test)]
    pub(crate) fn tracked_peers(&self) -> usize {
        self.pool.tracked.lock().unwrap().len()
    }

    /// The pooled connection's id, to tell whether it was replaced. For
    /// tests (`tests/addr_churn.rs` is an integration test, hence `pub`).
    #[doc(hidden)]
    pub async fn pooled_connection_id(&self, peer: iroh::EndpointId) -> Option<usize> {
        let gate = self.pool.gate(&peer)?;
        let id = gate.lock().await.as_ref().map(|conn| conn.stable_id());
        id
    }

    #[cfg(test)]
    pub(crate) async fn close_pooled_connection(&self, peer: iroh::EndpointId) {
        let gate = self.pool.gate(&peer);
        if let Some(gate) = gate {
            if let Some(conn) = gate.lock().await.as_ref() {
                conn.close(iroh::endpoint::VarInt::from_u32(0), b"test close");
            }
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The gossip actor, so the router can dispatch its ALPN.
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    pub fn secret_key(&self) -> &SecretKey {
        &self.key
    }
}

fn transport_observation(
    conn: &iroh::endpoint::Connection,
) -> (Option<std::time::Duration>, PathKind) {
    let paths = conn.paths();
    let selected = paths.iter().find(|path| path.is_selected());
    match selected {
        Some(path) if path.is_relay() => (Some(path.rtt()), PathKind::Relay),
        Some(path) if path.is_ip() => (Some(path.rtt()), PathKind::Direct),
        Some(path) => (Some(path.rtt()), PathKind::Unknown),
        None => (None, PathKind::Unknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_prefers_the_secret_over_the_uuid() {
        let secret = [7u8; 32];
        let from_secret = topic_for(Some(&secret), "uuid-a");
        assert_eq!(
            from_secret,
            topic_for(Some(&secret), "uuid-b"),
            "the secret alone determines the topic"
        );
        let a = topic_for(None, "uuid-a");
        let b = topic_for(None, "uuid-b");
        assert_ne!(a, b, "legacy filesystems get distinct topics per uuid");
        assert_ne!(from_secret, a);
        // Deterministic, so every node of one filesystem agrees.
        assert_eq!(a, topic_for(None, "uuid-a"));
    }

    /// End-to-end over real QUIC on loopback: a direct request reaches the
    /// peer, the peer's handler runs, and the signed reply verifies as
    /// coming from that peer. This is the piece the unit tests for
    /// `message`/`handoff` deliberately stub out.
    #[tokio::test]
    async fn direct_request_round_trip_over_quic() {
        let topic = topic_for(Some(&[1u8; 32]), "fs");
        let server = P2p::spawn(SecretKey::generate(), topic).await.unwrap();
        let client = P2p::spawn(SecretKey::generate(), topic).await.unwrap();
        // Each side allows the other, as the registry would.
        server.set_allowed([client.pubkey_hex()]);
        client.set_allowed([server.pubkey_hex()]);

        let server_key = server.secret_key().clone();
        let ep = server.endpoint().clone();
        let accept = tokio::spawn(async move {
            let incoming = ep.accept().await.expect("no inbound connection");
            let conn = incoming.await.unwrap();
            let (mut send, mut recv) = conn.accept_bi().await.unwrap();
            let req = crate::message::read_frame(&mut recv).await.unwrap();
            let (_, payload) = req.verify().unwrap();
            let reply = match payload {
                Payload::LeaseRequest { part, .. } => Payload::LeaseHandoff {
                    part,
                    epoch: 11,
                    released: true,
                    etag: None,
                    head_seq: None,
                },
                other => panic!("unexpected {other:?}"),
            };
            let signed = Signed::new(&server_key, &reply).unwrap();
            crate::message::write_frame(&mut send, &signed)
                .await
                .unwrap();
            send.finish().ok();
            // Keep the connection alive until the client has read.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let server_addr = server.addr();
        let reply = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            client.request(
                server_addr,
                &Payload::LeaseRequest {
                    part: "p0".into(),
                    requester: 2,
                    epoch_applied: None,
                },
            ),
        )
        .await
        .expect("request timed out")
        .expect("request failed");
        assert_eq!(
            crate::interpret_reply("p0", &reply),
            crate::RequestOutcome::ClaimNow
        );
        accept.abort();
    }
}
