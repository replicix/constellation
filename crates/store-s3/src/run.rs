//! GET-next over a sequentially keyed stream: the fetch shared by
//! [`crate::log::LogStore::get_run`], [`crate::commits::CommitChain::get_run`]
//! and [`crate::inbox::InboxStore::get_run`].
//!
//! Every GET is a spawned task rather than a future polled in place. A
//! run usually ends at its *first* key (a tail probe that finds nothing
//! new), and the in-place shape (`buffered(k)`, stop at the first miss)
//! then drops the other `k - 1` requests mid-flight. hyper cannot return
//! an HTTP/1 connection to the pool while a request on it is unfinished,
//! so it closes it: every probe cost up to `k` fresh TCP connections. At
//! `tail_width = 16`, three idle-ish nodes left ~2,000 sockets a second
//! in TIME-WAIT and exhausted the ephemeral port range within seconds,
//! after which every S3 request failed to connect. A spawned GET that
//! nobody awaits still runs to completion — reads its 404 body and hands
//! the connection back — while the caller still returns at the first gap
//! without waiting for the stragglers. The same holds if the caller
//! itself is cancelled.

use crate::error::StoreError;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;

/// Fetch `keys` concurrently (all in flight at once) and return the
/// bodies of the longest present prefix, in order. A `NotFound` ends the
/// run; replies past it are discarded unread by the caller, but still
/// drained by their own tasks.
pub(crate) async fn get_run(
    store: &Arc<dyn ObjectStore>,
    keys: impl IntoIterator<Item = Path>,
) -> Result<Vec<Bytes>, StoreError> {
    let fetches: Vec<_> = keys
        .into_iter()
        .map(|key| {
            let store = Arc::clone(store);
            tokio::spawn(async move {
                match store.get(&key).await {
                    Ok(res) => res.bytes().await.map(Some),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(e),
                }
            })
        })
        .collect();
    let mut run = Vec::new();
    // Dropping a `JoinHandle` detaches its task, so an early return below
    // leaves the rest to finish on their own.
    for fetch in fetches {
        match fetch.await {
            Ok(Ok(Some(body))) => run.push(body),
            Ok(Ok(None)) => break,
            Ok(Err(e)) => return Err(e.into()),
            Err(join) if join.is_panic() => std::panic::resume_unwind(join.into_panic()),
            Err(join) => {
                return Err(object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(join),
                }
                .into())
            }
        }
    }
    Ok(run)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    #[tokio::test]
    async fn stops_at_the_first_gap_in_key_order() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for i in [0u8, 1, 3] {
            store
                .put(&Path::from(format!("k/{i}")), PutPayload::from(vec![i]))
                .await
                .unwrap();
        }
        let keys = (0..5).map(|i| Path::from(format!("k/{i}")));
        let run = get_run(&store, keys).await.unwrap();
        assert_eq!(run, vec![Bytes::from(vec![0u8]), Bytes::from(vec![1u8])]);
        assert!(get_run(&store, Vec::new()).await.unwrap().is_empty());
    }
}
