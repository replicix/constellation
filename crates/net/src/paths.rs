//! Path visibility (plan 30 §M4 item 6): which network paths each peer
//! connection has open right now.
//!
//! iroh 1.x runs on `noq`, a QUIC implementation with multipath: one
//! connection keeps several paths open at once, typically the relay path
//! it started on and, once holepunching succeeds, a direct UDP path.
//! Application data goes over the *selected* path; the others stay open
//! and validated. [`PathSummary`] is a snapshot of that
//! (`iroh::endpoint::Connection::paths`), for `status`.
//!
//! # Failover from a direct path to a relay
//!
//! A peer that is reachable directly is normally used over the direct
//! path (lower latency; iroh selects it once holepunching validates it).
//! When that path fails — a NAT rebinding, a firewall change, a network
//! switch — the connection does not break: the relay path is still open
//! (`relay` ≥ 1 in the summary, `multipath: true` while both are up), so
//! noq moves traffic to it without a new handshake, the QUIC streams in
//! flight continue, and `selected` flips to `relay`. Constellation sees
//! this only as a higher round-trip time for that peer (the source
//! selector and lease placement use measured RTTs, never the path kind).
//! iroh keeps trying to re-establish a direct path in the background and
//! switches back when one validates. Only when *every* path is gone does
//! the connection close; the next request then dials afresh (the pooled
//! connection is dropped, `endpoint::P2p::connection`), and every caller
//! already falls back to S3 on a failed P2P request.
//!
//! A summary with no open path, or none at all, means no connection is
//! pooled: this node has not dialed that peer yet (it may still be
//! reached by the peer dialing in, which this snapshot does not cover).

use crate::endpoint::PathKind;
use std::time::Duration;

/// The open paths of one pooled peer connection (see the module doc).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathSummary {
    /// Kind of the selected path, if one is selected.
    pub selected: Option<PathKind>,
    /// Open direct (IP) paths.
    pub direct: u32,
    /// Open relay paths.
    pub relay: u32,
    /// Every open path's kind and RTT estimate, the selected one first.
    pub rtts: Vec<(PathKind, Duration)>,
    /// The selected path's remote socket address, when it is direct.
    pub selected_addr: Option<std::net::SocketAddr>,
    /// The selected path's local address, when it is direct and noq
    /// knows it.
    pub selected_local: Option<std::net::IpAddr>,
}

impl PathSummary {
    /// More than one path is open, so the selected one can fail over.
    pub fn multipath(&self) -> bool {
        self.direct + self.relay > 1
    }

    /// Snapshot `conn`'s open paths.
    pub fn of(conn: &iroh::endpoint::Connection) -> Self {
        let paths = conn.paths();
        let mut out = PathSummary::default();
        let mut rtts: Vec<(bool, PathKind, Duration)> = Vec::new();
        for path in paths.iter() {
            let kind = if path.is_relay() {
                out.relay += 1;
                PathKind::Relay
            } else if path.is_ip() {
                out.direct += 1;
                PathKind::Direct
            } else {
                PathKind::Unknown
            };
            if path.is_selected() {
                out.selected = Some(kind);
                if let iroh::TransportAddr::Ip(addr) = path.remote_addr() {
                    out.selected_addr = Some(*addr);
                    if let iroh::endpoint::LocalTransportAddr::Ip(local) = path.local_addr() {
                        out.selected_local = *local;
                    }
                }
            }
            rtts.push((path.is_selected(), kind, path.rtt()));
        }
        // Selected first, then in the order iroh lists them.
        rtts.sort_by_key(|(selected, _, _)| !*selected);
        out.rtts = rtts.into_iter().map(|(_, k, rtt)| (k, rtt)).collect();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipath_means_more_than_one_open_path() {
        let mut s = PathSummary::default();
        assert!(!s.multipath());
        s.relay = 1;
        assert!(!s.multipath());
        s.direct = 1;
        assert!(s.multipath());
    }
}
