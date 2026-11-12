//! Plan 30 §M4 item 2: the control-plane side of poison-record isolation
//! — `status`'s `held` section and `constellation repair drop-held <ino>`.
//! The mechanism itself is `constellation_meta::store::held`.

use constellation_meta::Meta;

/// `status.held`: what the last ship round held back, per poisoned inode.
pub fn status(meta: &Meta) -> constellation_control::proto::types::HeldStatus {
    let summary = meta.held_summary();
    // A poisoned inode with nothing journaled for it (its manifest shipped
    // before the chunk went missing, or was already dropped) still shows,
    // so the lost chunk is visible; the summary is only refreshed by a
    // ship round, so read the recorded marks directly too.
    let mut inodes = summary.inodes.clone();
    if let Ok(marks) = meta.unrecoverable_chunks() {
        for (hash, ino) in marks {
            let entry = inodes.entry(ino).or_default();
            if !entry.missing.contains(&hash) {
                entry.missing.push(hash);
            }
        }
    }
    let now_ms = replicix_objcas::lease::now_unix_ms();
    let remote = meta
        .remote_chunks()
        .unwrap_or_default()
        .into_iter()
        .map(|r| constellation_control::proto::types::RemoteChunkStatus {
            ino: r.ino,
            path: meta.path_of(r.ino).ok(),
            node: r.node,
            chunk: r.hash.to_hex(),
            age_s: (now_ms - r.enrolled_ms).max(0) as u64 / 1000,
        })
        .collect();
    let awaiting = meta
        .pending_replays()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|q| {
            let placeholder = q.await_retry?;
            Some(constellation_control::proto::types::AwaitingRetryStatus {
                node: q.rid.node,
                rid: format!("{}.{}.{}", q.rid.node, q.rid.incarnation, q.rid.seq),
                age_s: (now_ms - placeholder.since_ms).max(0) as u64 / 1000,
                keys: (placeholder.keys.dentries.len()
                    + placeholder.keys.inos.len()
                    + placeholder.keys.shared.len()) as u64,
            })
        })
        .collect();
    constellation_control::proto::types::HeldStatus {
        transactions: summary.transactions,
        records: summary.records,
        oldest_seq: summary.oldest_seq,
        opaque: summary.opaque,
        deferred: summary.deferred,
        drop_sessions_deferred: 0,
        remote,
        awaiting,
        inodes: inodes
            .into_iter()
            .map(
                |(ino, held)| constellation_control::proto::types::HeldInodeStatus {
                    ino,
                    path: meta.path_of(ino).ok(),
                    missing_chunks: held.missing.iter().map(|h| h.to_hex()).collect(),
                    seeds: held.seeds,
                },
            )
            .collect(),
    }
}
