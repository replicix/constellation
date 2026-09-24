//! Plan 30 §M4 item 2: the control-plane side of poison-record isolation
//! — `status`'s `held` section and `constellation repair drop-held <ino>`.
//! The mechanism itself is `constellation_meta::store::held`.

use constellation_meta::Meta;

/// `status.held`: what the last ship round held back, per poisoned inode.
pub fn status(meta: &Meta) -> constellation_api::HeldStatus {
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
    constellation_api::HeldStatus {
        transactions: summary.transactions,
        records: summary.records,
        oldest_seq: summary.oldest_seq,
        opaque: summary.opaque,
        deferred: summary.deferred,
        inodes: inodes
            .into_iter()
            .map(|(ino, held)| constellation_api::HeldInodeStatus {
                ino,
                path: meta.path_of(ino).ok(),
                missing_chunks: held.missing.iter().map(|h| h.to_hex()).collect(),
                seeds: held.seeds,
            })
            .collect(),
    }
}

/// `constellation repair drop-held <ino>`: roll the inode's held records
/// back, queue what depended on them for replay by rid, and leave the
/// conflict copies to the replay drain (`recovery::drain_pending_replays`
/// materializes each refused replay within a tick or so).
pub fn drop_held(meta: &Meta, ino: u64) -> Result<String, String> {
    let now = constellation_fs_core::types::now_ns() / 1_000_000_000;
    let dropped = meta.drop_held(ino, now).map_err(|e| e.to_string())?;
    Ok(format!(
        "inode {ino}: {} held transaction(s) dropped into a conflict copy under \
         {}/ (lost chunks become holes), {} dependent transaction(s) rolled back and queued \
         for replay, {} queued replay(s) dropped, {} unrecoverable pending upload(s) removed",
        dropped.dropped,
        constellation_meta::CONFLICT_DIR,
        dropped.requeued,
        dropped.queued_dropped,
        dropped.pending_removed
    ))
}
