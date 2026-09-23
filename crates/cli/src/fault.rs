//! Test-only fault points (`CONSTELLATION_FAULT_*`), for harness scenarios
//! that need a failure at a precise point rather than a race against the
//! daemon's own timing (plan 30 §M4 round 2, `poison-record-isolation`).
//! Unset, each is one cached environment lookup and changes nothing.

use constellation_fs_core::ChunkHash;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::OnceLock;

/// `CONSTELLATION_FAULT_LOSE_CHUNKS=<hex>[,<hex>...]`: the upload pass
/// deletes these chunks from the local cache right before reading them,
/// so a pending upload of one finds its content gone — the plan 29 M6
/// "missing from local cache" condition, at exactly the point the upload
/// pass would notice it, instead of a harness deleting the cache file and
/// racing the upload round that may already hold the bytes.
pub fn lose_chunk(hash: &ChunkHash) -> bool {
    static LOST: OnceLock<HashSet<ChunkHash>> = OnceLock::new();
    LOST.get_or_init(|| {
        std::env::var("CONSTELLATION_FAULT_LOSE_CHUNKS")
            .ok()
            .map(|v| {
                v.split(',')
                    .filter_map(|h| ChunkHash::from_hex(h.trim()))
                    .collect()
            })
            .unwrap_or_default()
    })
    .contains(hash)
}

/// `CONSTELLATION_FAULT_HOLD_SYNC_FILE=<path>`: while that file exists,
/// every managed sync round returns at once — nothing is uploaded, shipped
/// or published — and the round writes `<path>.held` to say a round has
/// observed the hold, so the harness knows no round is still in flight
/// when it goes on (the sync task runs its rounds one at a time).
pub fn sync_held() -> bool {
    static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    let Some(path) = PATH
        .get_or_init(|| std::env::var_os("CONSTELLATION_FAULT_HOLD_SYNC_FILE").map(PathBuf::from))
    else {
        return false;
    };
    if !path.exists() {
        return false;
    }
    let mut marker = path.clone().into_os_string();
    marker.push(".held");
    let _ = std::fs::write(PathBuf::from(marker), b"held");
    true
}
