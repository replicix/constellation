//! Which local addresses a node advertises, and which paths it prefers.
//!
//! iroh binds the unspecified address and offers peers one address per
//! up, non-link-local interface — on a container host that includes every
//! docker bridge, which other jobs create and remove every few seconds.
//! Two nodes on such a host ended up talking over whichever of those
//! addresses iroh happened to select, and when that bridge was removed
//! the link stalled for 15 s to minutes: noq re-opens a path whose
//! address became unusable *to the same remote*, marks it validated and
//! keeps its status, so with the vanished address having been the
//! selected one, application data keeps being scheduled onto a black
//! hole until that path's idle timeout. Bridge churn elsewhere on the
//! host kept resetting the race.
//!
//! Two things keep a link off such addresses:
//!
//! * [`AddrPolicy`]: the address published in the registry carries only
//!   addresses on admitted interfaces (`CONSTELLATION_P2P_INTERFACES`,
//!   `CONSTELLATION_P2P_INTERFACES_DENY`; container plumbing is denied by
//!   default). The registry is what peers dial, so a first path lands on
//!   one of them.
//! * [`PreferredPaths`]: iroh still exchanges *every* local address in-band
//!   for holepunching, so paths to bridge addresses open anyway. This
//!   path selector ranks paths to a peer's published addresses above
//!   every other direct path. The others stay open as backups — noq sends
//!   no data on a backup path while an available one exists, so losing
//!   one costs nothing — and are still used when nothing better is left.
//!
//! A connection whose selected path dies because one of *our* admitted
//! addresses went away is reset at once by `endpoint`'s interface watch.
//! One the watch cannot see (the *peer's* address went away) loses that
//! path after the path idle timeout in `endpoint`
//! (`CONSTELLATION_P2P_PATH_IDLE_MS`, 5 s; iroh's is 15 s) — but only
//! while another path is open: noq never abandons a connection's last
//! path on a local timer. A connection down to its last path (the usual
//! case between hosts) is caught by `endpoint`'s probe of a pooled
//! connection whose request went unanswered (3 s), or else by iroh's
//! 30 s connection idle timeout.

use iroh::endpoint::transports::{Addr, PathSelection, PathSelectionContext, PathSelector};
use iroh::{EndpointAddr, EndpointId, TransportAddr};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::RwLock;
use std::time::Duration;

/// Interfaces never advertised unless `CONSTELLATION_P2P_INTERFACES_DENY`
/// says otherwise: container and VM plumbing, whose addresses come and go
/// with other people's workloads and are only reachable from this host.
/// VPN and overlay tunnels (`tun*`, `wg*`, `tailscale*`, `zt*`) are
/// admitted on purpose: they are often the only route between two
/// fleet hosts, and they come and go with the host, not with workloads.
pub const DEFAULT_DENY: &[&str] = &[
    "docker*", "br-*", "veth*", "virbr*", "vnet*", "cni*", "flannel*", "cali*", "vxlan*",
    "cilium*", "weave*", "podman*", "lxcbr*", "lxdbr*", "kube-*",
];

/// Which local interfaces' addresses this node publishes as its P2P
/// address. An interface is admitted when it matches an allow pattern
/// and no deny pattern. Patterns are interface names with `*` and `?`
/// wildcards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddrPolicy {
    allow: Vec<String>,
    deny: Vec<String>,
}

impl Default for AddrPolicy {
    fn default() -> Self {
        Self::parse(None, None)
    }
}

impl AddrPolicy {
    /// * `CONSTELLATION_P2P_INTERFACES`: comma-separated patterns to
    ///   advertise; unset, empty or `*` admits every interface.
    /// * `CONSTELLATION_P2P_INTERFACES_DENY`: comma-separated patterns
    ///   never advertised; unset is [`DEFAULT_DENY`], empty or `none`
    ///   denies nothing.
    pub fn from_env() -> Self {
        let allow = std::env::var("CONSTELLATION_P2P_INTERFACES").ok();
        let deny = std::env::var("CONSTELLATION_P2P_INTERFACES_DENY").ok();
        Self::parse(allow.as_deref(), deny.as_deref())
    }

    pub fn parse(allow: Option<&str>, deny: Option<&str>) -> Self {
        let list = |raw: &str| -> Vec<String> {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        };
        let allow = allow.map(list).unwrap_or_default();
        let deny = match deny {
            None => DEFAULT_DENY.iter().map(|s| s.to_string()).collect(),
            Some(raw) if raw.trim().eq_ignore_ascii_case("none") => Vec::new(),
            Some(raw) => list(raw),
        };
        Self { allow, deny }
    }

    /// Whether addresses on interface `name` may be advertised.
    pub fn admits(&self, name: &str) -> bool {
        (self.allow.is_empty() || self.allow.iter().any(|p| glob(p, name)))
            && !self.deny.iter().any(|p| glob(p, name))
    }

    /// What this node publishes, given `addr` (iroh's own view of its
    /// addresses) and the local interfaces as `(name, addresses)`: the
    /// direct addresses an admitted interface holds, and the relay URLs.
    ///
    /// Direct addresses no local interface holds are dropped: those are
    /// what a relay or a peer *observed* (a NAT mapping), which iroh
    /// re-learns and may change whenever its relay reconnects. Published,
    /// each such change would re-publish the record and have every peer
    /// re-check its connection to us; peers learn them in-band anyway
    /// (iroh exchanges every candidate for holepunching once a first path,
    /// relay or direct, is up). The relay URL is kept: a peer that cannot
    /// reach any direct address needs it to dial at all.
    ///
    /// If no admitted interface holds an address, every locally held one
    /// is published instead: a peer that can only be reached over a bridge
    /// is still better reached that way than not at all. If iroh knows no
    /// locally held address either, `addr` is returned whole.
    pub fn filter(
        &self,
        addr: &EndpointAddr,
        interfaces: &[(String, Vec<IpAddr>)],
    ) -> EndpointAddr {
        let held = |ip: IpAddr, admitted_only: bool| {
            let ip = ip.to_canonical();
            interfaces.iter().any(|(name, ips)| {
                (!admitted_only || self.admits(name)) && ips.iter().any(|i| i.to_canonical() == ip)
            })
        };
        let pick = |admitted_only: bool| -> std::collections::BTreeSet<TransportAddr> {
            addr.addrs
                .iter()
                .filter(|a| match a {
                    TransportAddr::Ip(sa) => held(sa.ip(), admitted_only),
                    _ => true,
                })
                .cloned()
                .collect()
        };
        let has_direct = |set: &std::collections::BTreeSet<TransportAddr>| {
            set.iter().any(|a| matches!(a, TransportAddr::Ip(_)))
        };
        let mut kept = pick(true);
        if !has_direct(&kept) {
            kept = pick(false);
        }
        if !has_direct(&kept) && has_direct(&addr.addrs) {
            return addr.clone();
        }
        EndpointAddr {
            id: addr.id,
            addrs: kept,
        }
    }

    /// Every address on an admitted interface that a path could use: not
    /// loopback, not link-local. What [`crate::endpoint`] watches for losses.
    pub fn admitted_local_ips(&self, interfaces: &[(String, Vec<IpAddr>)]) -> HashSet<IpAddr> {
        interfaces
            .iter()
            .filter(|(name, _)| self.admits(name))
            .flat_map(|(_, ips)| ips.iter().map(|ip| ip.to_canonical()))
            .filter(|ip| !ip.is_loopback() && !is_link_local(*ip))
            .collect()
    }

    /// [`Self::filter`] against the interfaces as they are now.
    pub async fn filter_now(&self, addr: &EndpointAddr) -> EndpointAddr {
        self.filter(addr, &local_interfaces().await)
    }
}

/// The direct addresses of `addr`, canonical (IPv4-mapped IPv6 as IPv4).
/// What decides whether a peer's re-published record moved it.
pub fn direct_addrs(addr: &EndpointAddr) -> std::collections::BTreeSet<SocketAddr> {
    addr.addrs
        .iter()
        .filter_map(|a| match a {
            TransportAddr::Ip(sa) => Some(canonical(*sa)),
            _ => None,
        })
        .collect()
}

/// Whether a path from `local` to `remote` ran over one of the `lost`
/// local addresses: `Some(true)` if either end is one (a peer on this
/// host is reached at one of our own addresses), `Some(false)` if
/// neither is and the local end is known, `None` if the local end is
/// unknown (noq does not always learn which source address the kernel
/// picked) and the remote is not lost.
pub fn path_uses_lost(
    remote: Option<IpAddr>,
    local: Option<IpAddr>,
    lost: &HashSet<IpAddr>,
) -> Option<bool> {
    let lost_ip = |ip: IpAddr| lost.contains(&ip.to_canonical());
    if remote.is_some_and(lost_ip) || local.is_some_and(lost_ip) {
        return Some(true);
    }
    match (remote, local) {
        (Some(_), Some(_)) => Some(false),
        _ => None,
    }
}

/// Every local interface that is up, and its addresses.
pub async fn local_interfaces() -> Vec<(String, Vec<IpAddr>)> {
    interfaces_of(&netwatch::interfaces::State::new().await)
}

/// The interfaces of `state` that are up, and their addresses.
pub fn interfaces_of(state: &netwatch::interfaces::State) -> Vec<(String, Vec<IpAddr>)> {
    state
        .interfaces
        .values()
        .filter(|i| i.is_up())
        .map(|i| {
            (
                i.name().to_string(),
                i.addrs().map(|net| net.addr()).collect(),
            )
        })
        .collect()
}

fn is_link_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => v6.segments()[0] & 0xffc0 == 0xfe80,
    }
}

/// `*` matches any run of characters, `?` any one.
fn glob(pattern: &str, name: &str) -> bool {
    fn go(p: &[u8], n: &[u8]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => go(&p[1..], n) || (!n.is_empty() && go(p, &n[1..])),
            (Some(b'?'), Some(_)) => go(&p[1..], &n[1..]),
            (Some(a), Some(b)) if a == b => go(&p[1..], &n[1..]),
            _ => false,
        }
    }
    go(pattern.as_bytes(), name.as_bytes())
}

/// IPv6 is preferred by this much RTT at equal rank, as iroh's own
/// selector does.
const IPV6_RTT_ADVANTAGE: Duration = Duration::from_millis(3);
/// A path of the same rank must be this much faster to take over (no
/// flapping under jitter), as in iroh's own selector.
const RTT_SWITCHING_MIN: Duration = Duration::from_millis(5);

/// The path selector installed on every endpoint (see the module docs):
/// iroh's biased-RTT selection, with a rank in front of the RTT — direct
/// paths to an address the peer published first, then other direct
/// paths, then relays.
#[derive(Debug, Default)]
pub struct PreferredPaths {
    /// Each peer's published direct addresses, as last learned from the
    /// registry.
    published: RwLock<HashMap<EndpointId, Vec<SocketAddr>>>,
    /// Their union, which is all [`Self::select`] needs.
    preferred: RwLock<HashSet<SocketAddr>>,
}

impl PreferredPaths {
    /// Record `addr` as what its peer published.
    pub fn learn(&self, addr: &EndpointAddr) {
        let ips: Vec<SocketAddr> = addr
            .addrs
            .iter()
            .filter_map(|a| match a {
                TransportAddr::Ip(sa) => Some(canonical(*sa)),
                _ => None,
            })
            .collect();
        let mut published = self.published.write().unwrap();
        if published.get(&addr.id) == Some(&ips) {
            return;
        }
        published.insert(addr.id, ips);
        *self.preferred.write().unwrap() = published.values().flatten().copied().collect();
    }

    fn is_preferred(&self, addr: SocketAddr) -> bool {
        self.preferred.read().unwrap().contains(&canonical(addr))
    }

    /// Lower is better: `(rank, biased RTT in ns)`.
    fn key(&self, remote: &Addr, rtt: Duration) -> (u8, i128) {
        let rank = match remote {
            Addr::Ip(sa) if self.is_preferred(*sa) => 0,
            Addr::Ip(_) => 1,
            _ => 2,
        };
        let mut biased = rtt.as_nanos() as i128;
        if matches!(remote, Addr::Ip(sa) if sa.ip().to_canonical().is_ipv6()) {
            biased -= IPV6_RTT_ADVANTAGE.as_nanos() as i128;
        }
        (rank, biased)
    }

    /// Pick from `(remote, rtt)` candidates given the current one;
    /// `None` keeps the current selection. Split out of [`Self::select`]
    /// for tests.
    fn choose<'p>(
        &self,
        current: Option<&Addr>,
        paths: impl Iterator<Item = (&'p Addr, Duration)>,
    ) -> Option<usize> {
        let mut best: Option<(usize, (u8, i128))> = None;
        let mut current_key: Option<(u8, i128)> = None;
        for (i, (remote, rtt)) in paths.enumerate() {
            let key = self.key(remote, rtt);
            if Some(remote) == current && current_key.is_none_or(|c| key < c) {
                current_key = Some(key);
            }
            if best.is_none_or(|(_, b)| key < b) {
                best = Some((i, key));
            }
        }
        let (index, (rank, biased)) = best?;
        match current_key {
            None => Some(index),
            Some((current_rank, _)) if current_rank != rank => Some(index),
            Some((_, current_biased))
                if biased + RTT_SWITCHING_MIN.as_nanos() as i128 <= current_biased =>
            {
                Some(index)
            }
            Some(_) => None,
        }
    }
}

pub(crate) fn canonical(sa: SocketAddr) -> SocketAddr {
    SocketAddr::new(sa.ip().to_canonical(), sa.port())
}

impl PathSelector for PreferredPaths {
    fn select(&self, ctx: &PathSelectionContext<'_>) -> PathSelection {
        let current = ctx.current().map(|p| p.remote());
        let candidates: Vec<_> = ctx
            .paths()
            .filter_map(|psd| {
                // Skip paths whose stats can't be read (closed concurrently).
                let rtt = psd.stats()?.rtt;
                Some((psd, rtt))
            })
            .collect();
        let remotes: Vec<Addr> = candidates
            .iter()
            .map(|(psd, _)| psd.network_path().remote())
            .collect();
        let chosen = self.choose(
            current.as_ref(),
            remotes
                .iter()
                .zip(&candidates)
                .map(|(remote, (_, rtt))| (remote, *rtt)),
        );
        let mut selection = PathSelection::none();
        if let Some(i) = chosen {
            selection.set(&candidates[i].0);
        }
        selection
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ifaces() -> Vec<(String, Vec<IpAddr>)> {
        vec![
            ("lo".into(), vec!["127.0.0.1".parse().unwrap()]),
            ("eth0".into(), vec!["10.0.0.5".parse().unwrap()]),
            ("docker0".into(), vec!["172.17.0.1".parse().unwrap()]),
            (
                "br-c4f6e561eff8".into(),
                vec!["172.18.0.1".parse().unwrap()],
            ),
            ("veth1234".into(), vec!["fe80::1".parse().unwrap()]),
        ]
    }

    fn endpoint(addrs: &[&str]) -> EndpointAddr {
        EndpointAddr {
            id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            addrs: addrs
                .iter()
                .map(|a| TransportAddr::Ip(a.parse().unwrap()))
                .collect(),
        }
    }

    fn ips(addr: &EndpointAddr) -> Vec<String> {
        addr.addrs.iter().map(|a| format!("{a:?}")).collect()
    }

    #[test]
    fn glob_matches_names() {
        assert!(glob("br-*", "br-c4f6e561eff8"));
        assert!(glob("docker*", "docker0"));
        assert!(glob("eth?", "eth0"));
        assert!(!glob("eth?", "eth10"));
        assert!(glob("*", "anything"));
        assert!(!glob("br-*", "bridge0"));
    }

    #[test]
    fn container_plumbing_is_not_advertised_by_default() {
        let policy = AddrPolicy::parse(None, None);
        for name in [
            "docker0",
            "br-c4f6e561eff8",
            "veth46f4ae9",
            "virbr0",
            "cni0",
            "cali123",
        ] {
            assert!(!policy.admits(name), "{name} admitted");
        }
        for name in ["eth0", "enp39s0", "wlan0", "tailscale0", "lo"] {
            assert!(policy.admits(name), "{name} refused");
        }
        let addr = endpoint(&["10.0.0.5:7000", "172.17.0.1:7000", "172.18.0.1:7000"]);
        let filtered = policy.filter(&addr, &ifaces());
        assert_eq!(ips(&filtered), ips(&endpoint(&["10.0.0.5:7000"])));
    }

    #[test]
    fn observed_addresses_are_dropped_and_relays_kept() {
        let policy = AddrPolicy::parse(None, None);
        let mut addr = endpoint(&["203.0.113.7:7000", "172.17.0.1:7000", "10.0.0.5:7000"]);
        let relay: iroh::RelayUrl = "https://relay.example.com".parse().unwrap();
        addr.addrs.insert(TransportAddr::Relay(relay.clone()));
        let filtered = policy.filter(&addr, &ifaces());
        let mut want = endpoint(&["10.0.0.5:7000"]);
        want.addrs.insert(TransportAddr::Relay(relay.clone()));
        assert_eq!(filtered, want);
        // A NAT mapping that moves (the relay reconnected) publishes the
        // same record: nothing to re-publish, nobody re-checks us.
        let mut moved = endpoint(&["203.0.113.7:7111", "172.17.0.1:7000", "10.0.0.5:7000"]);
        moved.addrs.insert(TransportAddr::Relay(relay));
        assert_eq!(policy.filter(&moved, &ifaces()), filtered);
    }

    #[test]
    fn only_bridges_publishes_the_bridges_not_observed_addresses() {
        let policy = AddrPolicy::parse(None, None);
        let addr = endpoint(&["203.0.113.7:7000", "172.17.0.1:7000"]);
        assert_eq!(
            policy.filter(&addr, &ifaces()),
            endpoint(&["172.17.0.1:7000"])
        );
        // Nothing held locally at all: publish what iroh has.
        let foreign = endpoint(&["203.0.113.7:7000"]);
        assert_eq!(policy.filter(&foreign, &ifaces()), foreign);
    }

    #[test]
    fn a_path_uses_a_lost_address_at_either_end() {
        let lost: HashSet<IpAddr> = ["10.99.0.2".parse().unwrap()].into();
        let ip = |s: &str| Some(s.parse::<IpAddr>().unwrap());
        // A peer on this host reached at the lost address.
        assert_eq!(path_uses_lost(ip("10.99.0.2"), None, &lost), Some(true));
        assert_eq!(
            path_uses_lost(ip("::ffff:10.99.0.2"), ip("10.98.0.2"), &lost),
            Some(true)
        );
        // Sent from the lost address.
        assert_eq!(
            path_uses_lost(ip("10.0.0.6"), ip("10.99.0.2"), &lost),
            Some(true)
        );
        // Neither end: unaffected.
        assert_eq!(
            path_uses_lost(ip("10.98.0.2"), ip("10.98.0.2"), &lost),
            Some(false)
        );
        // Local end unknown: undecided.
        assert_eq!(path_uses_lost(ip("10.98.0.2"), None, &lost), None);
        assert_eq!(path_uses_lost(None, None, &lost), None);
    }

    #[test]
    fn direct_addrs_ignore_relays_and_mapping() {
        let mut addr = endpoint(&["[::ffff:10.0.0.5]:7000"]);
        addr.addrs.insert(TransportAddr::Relay(
            "https://relay.example.com".parse().unwrap(),
        ));
        assert_eq!(
            direct_addrs(&addr),
            ["10.0.0.5:7000".parse().unwrap()].into()
        );
    }

    #[test]
    fn nothing_left_means_everything_is_kept() {
        let policy = AddrPolicy::parse(None, None);
        let addr = endpoint(&["172.17.0.1:7000", "172.18.0.1:7000"]);
        assert_eq!(policy.filter(&addr, &ifaces()), addr);
    }

    #[test]
    fn admitted_local_ips_skip_refused_loopback_and_link_local() {
        let policy = AddrPolicy::parse(None, None);
        let mut interfaces = ifaces();
        interfaces.push((
            "eth1".into(),
            vec!["fe80::2".parse().unwrap(), "169.254.1.1".parse().unwrap()],
        ));
        let ips: Vec<IpAddr> = policy.admitted_local_ips(&interfaces).into_iter().collect();
        assert_eq!(ips, vec!["10.0.0.5".parse::<IpAddr>().unwrap()]);
        let none = AddrPolicy::parse(Some("wlan*"), None);
        assert!(none.admitted_local_ips(&interfaces).is_empty());
    }

    #[test]
    fn allow_and_deny_knobs() {
        let only_eth = AddrPolicy::parse(Some("eth*, en*"), None);
        assert!(only_eth.admits("eth0") && only_eth.admits("enp39s0"));
        assert!(!only_eth.admits("wlan0") && !only_eth.admits("docker0"));
        let deny_none = AddrPolicy::parse(None, Some("none"));
        assert!(deny_none.admits("docker0"));
        let deny_empty = AddrPolicy::parse(Some("*"), Some(""));
        assert!(deny_empty.admits("br-1"));
        let deny_custom = AddrPolicy::parse(None, Some("wlan*"));
        assert!(!deny_custom.admits("wlan0") && deny_custom.admits("docker0"));
    }

    fn ip(s: &str) -> Addr {
        Addr::Ip(s.parse().unwrap())
    }

    fn pick(sel: &PreferredPaths, current: Option<&Addr>, paths: &[(Addr, u64)]) -> Option<Addr> {
        sel.choose(
            current,
            paths.iter().map(|(a, ms)| (a, Duration::from_millis(*ms))),
        )
        .map(|i| paths[i].0.clone())
    }

    #[test]
    fn published_addresses_win_over_faster_bridges() {
        let sel = PreferredPaths::default();
        sel.learn(&endpoint(&["10.0.0.5:7000"]));
        let stable = ip("10.0.0.5:7000");
        let bridge = ip("172.18.0.1:7000");
        // A much faster unpublished path does not take over...
        assert_eq!(
            pick(&sel, None, &[(bridge.clone(), 1), (stable.clone(), 40)]),
            Some(stable.clone())
        );
        assert_eq!(
            pick(
                &sel,
                Some(&stable),
                &[(bridge.clone(), 1), (stable.clone(), 40)]
            ),
            None
        );
        // ...and a selected one is left for a published one at once.
        assert_eq!(
            pick(
                &sel,
                Some(&bridge),
                &[(bridge.clone(), 1), (stable.clone(), 40)]
            ),
            Some(stable)
        );
        // With no published path left, any direct path beats nothing.
        assert_eq!(pick(&sel, None, &[(bridge.clone(), 1)]), Some(bridge));
    }

    #[test]
    fn mapped_ipv6_is_matched_canonically() {
        let sel = PreferredPaths::default();
        sel.learn(&endpoint(&["10.0.0.5:7000"]));
        let mapped = ip("[::ffff:10.0.0.5]:7000");
        assert_eq!(
            pick(
                &sel,
                None,
                &[(ip("172.18.0.1:7000"), 1), (mapped.clone(), 9)]
            ),
            Some(mapped)
        );
    }

    #[test]
    fn same_rank_keeps_iroh_stickiness() {
        let sel = PreferredPaths::default();
        sel.learn(&endpoint(&["10.0.0.5:7000", "10.0.1.5:7000"]));
        let (a, b) = (ip("10.0.0.5:7000"), ip("10.0.1.5:7000"));
        assert_eq!(
            pick(&sel, Some(&a), &[(a.clone(), 20), (b.clone(), 16)]),
            None
        );
        assert_eq!(
            pick(&sel, Some(&a), &[(a.clone(), 20), (b.clone(), 15)]),
            Some(b)
        );
    }

    #[test]
    fn relearning_replaces_a_peers_addresses() {
        let sel = PreferredPaths::default();
        sel.learn(&endpoint(&["10.0.0.5:7000"]));
        sel.learn(&endpoint(&["10.0.0.6:7000"]));
        assert!(!sel.is_preferred("10.0.0.5:7000".parse().unwrap()));
        assert!(sel.is_preferred("10.0.0.6:7000".parse().unwrap()));
    }
}
