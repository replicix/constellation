//! Plan 30 §M4 item 6: a peer's open network paths in `status`'s shape
//! (the snapshot itself is `constellation_net::paths`).

/// `status.p2p.peers[].paths` for one peer.
pub fn status(
    summary: Option<constellation_net::PathSummary>,
) -> constellation_api::PeerPathsStatus {
    let Some(s) = summary else {
        return constellation_api::PeerPathsStatus::default();
    };
    constellation_api::PeerPathsStatus {
        selected: s
            .selected
            .map(|k| k.as_str().to_string())
            .unwrap_or_default(),
        direct: s.direct,
        relay: s.relay,
        multipath: s.multipath(),
        rtts: s
            .rtts
            .iter()
            .map(|(kind, rtt)| format!("{}:{}", kind.as_str(), rtt.as_millis()))
            .collect(),
    }
}
