//! Bounded, parallel enumeration of content-addressed chunk keys.
//!
//! The two leading hex pairs in the chunk layout are not needed for AWS
//! request distribution, but the first pair gives 256 independent LIST
//! streams. Keeping parsing here prevents the mount from depending on S3
//! key-shape details while keeping P2P types out of the store crate.

use constellation_fs_core::ChunkHash;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::StoreError;

#[derive(Debug, Clone, Copy)]
pub struct ChunkHashScan {
    pub listed: usize,
    /// False means the caller's capacity was exceeded. The retained prefix
    /// remains useful for hits, but must never prove absence.
    pub complete: bool,
}

/// LIST every `chunks/<aa>/` shard with bounded request concurrency.
///
/// `limit` is the maximum number of hashes retained. One extra valid key is
/// enough to mark the result incomplete; remaining pages are then discarded.
pub async fn scan_chunk_hashes<F>(
    store: Arc<dyn ObjectStore>,
    limit: usize,
    concurrency: usize,
    visit: F,
) -> Result<ChunkHashScan, StoreError>
where
    F: Fn(ChunkHash) + Send + Sync + 'static,
{
    let listed = Arc::new(AtomicUsize::new(0));
    let complete = Arc::new(AtomicBool::new(true));
    let visit = Arc::new(visit);
    let shards = stream::iter(0u16..=255).map(|shard| {
        let store = store.clone();
        let listed = listed.clone();
        let complete = complete.clone();
        let visit = visit.clone();
        async move {
            let prefix = Path::from(format!("chunks/{shard:02x}"));
            store
                .list(Some(&prefix))
                .try_for_each(|meta| {
                    if let Some(hash) = parse_chunk_key(meta.location.as_ref()) {
                        match listed.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                            (n < limit).then_some(n + 1)
                        }) {
                            Ok(_) => visit(hash),
                            Err(_) => complete.store(false, Ordering::Relaxed),
                        }
                    }
                    futures::future::ready(Ok(()))
                })
                .await
        }
    });
    shards
        .buffer_unordered(concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(ChunkHashScan {
        listed: listed.load(Ordering::Relaxed),
        complete: complete.load(Ordering::Relaxed),
    })
}

/// Parse only canonical `chunks/<aa>/<bb>/<64-hex>` keys. Matching the
/// directory pairs against the hash avoids admitting unrelated objects
/// planted below the chunk prefix.
pub fn parse_chunk_key(key: &str) -> Option<ChunkHash> {
    let mut parts = key.split('/');
    let (Some("chunks"), Some(a), Some(b), Some(hex), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    if a.len() != 2 || b.len() != 2 || hex.len() != 64 || a != &hex[..2] || b != &hex[2..4] {
        return None;
    }
    ChunkHash::from_hex(hex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{memory::InMemory, ObjectStoreExt, PutPayload};

    #[test]
    fn canonical_key_round_trips_and_junk_is_ignored() {
        let hash = ChunkHash::of(b"round trip");
        let hex = hash.to_hex();
        let key = format!("chunks/{}/{}/{}", &hex[..2], &hex[2..4], hex);
        assert_eq!(parse_chunk_key(&key), Some(hash));
        assert_eq!(parse_chunk_key(&format!("chunks/00/00/{hex}")), None);
        assert_eq!(parse_chunk_key("chunks/aa/bb/not-a-hash"), None);
        assert_eq!(parse_chunk_key("other/aa/bb/00"), None);
    }

    #[tokio::test]
    async fn listing_skips_junk_and_marks_capacity_overflow_incomplete() {
        let store = Arc::new(InMemory::new());
        let hashes = [ChunkHash::of(b"a"), ChunkHash::of(b"b")];
        for hash in hashes {
            store
                .put(
                    &crate::layout::chunk_key(&hash),
                    PutPayload::from_static(b"x"),
                )
                .await
                .unwrap();
        }
        store
            .put(
                &Path::from("chunks/00/00/junk"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();

        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let all = scan_chunk_hashes(store.clone(), 8, 2, move |hash| {
            sink.lock().unwrap().push(hash);
        })
        .await
        .unwrap();
        assert!(all.complete);
        assert_eq!(all.listed, 2);
        assert!(seen
            .lock()
            .unwrap()
            .iter()
            .all(|hash| hashes.contains(hash)));

        let capped = scan_chunk_hashes(store, 1, 2, |_| {}).await.unwrap();
        assert!(!capped.complete);
        assert_eq!(capped.listed, 1);
    }
}
