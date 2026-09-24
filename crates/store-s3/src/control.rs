//! Reads of small JSON control objects on hot paths: the lease, node
//! registry records, designations.
//!
//! On real S3 a GET is atomic: it returns one complete version of the
//! object. Emulators need not be — floci was seen answering a lease GET
//! with a torn body (`json: EOF while parsing an object at line 1 column
//! 187`) while another node's conditional PUT of the same key was in
//! flight. A body that does not parse is therefore treated as a
//! *transient* read error: re-read promptly, a few times, with a short
//! backoff ([`TORN_READ_ATTEMPTS`], from [`TORN_READ_BACKOFF`] doubling),
//! and only a body that still does not parse after that is reported (as
//! the [`StoreError::Json`] it always was). The same goes for a body
//! stream that breaks off mid-read. A genuinely corrupt object costs the
//! retries (well under a second) and then fails exactly as before.

use crate::error::StoreError;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use serde::de::DeserializeOwned;
use std::time::Duration;

/// Reads of one control object before an unparseable body is final.
pub const TORN_READ_ATTEMPTS: u32 = 4;
/// First pause between re-reads (doubling: 20, 40, 80 ms).
pub const TORN_READ_BACKOFF: Duration = Duration::from_millis(20);

/// GET `path` and parse it as JSON, re-reading a torn body (see the
/// module doc). `Ok(None)` if the object does not exist (also when it is
/// deleted between re-reads).
pub async fn get_json<T: DeserializeOwned>(
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<Option<(T, ObjectMeta)>, StoreError> {
    let mut backoff = TORN_READ_BACKOFF;
    let mut attempt = 1u32;
    loop {
        let res = match store.get(path).await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let meta = res.meta.clone();
        let error: StoreError = match res.bytes().await {
            Ok(bytes) => match serde_json::from_slice::<T>(&bytes) {
                Ok(value) => return Ok(Some((value, meta))),
                Err(e) => {
                    if attempt < TORN_READ_ATTEMPTS {
                        tracing::debug!(
                            %path,
                            attempt,
                            bytes = bytes.len(),
                            error = %e,
                            "control object did not parse (a torn read?); re-reading"
                        );
                    }
                    e.into()
                }
            },
            Err(e) => {
                if attempt < TORN_READ_ATTEMPTS {
                    tracing::debug!(%path, attempt, error = %e, "control object body broke off; re-reading");
                }
                e.into()
            }
        };
        if attempt >= TORN_READ_ATTEMPTS {
            tracing::warn!(%path, attempts = attempt, error = %error, "control object unreadable");
            return Err(error);
        }
        attempt += 1;
        tokio::time::sleep(backoff).await;
        backoff *= 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
    use object_store::PutPayload;
    use std::sync::Arc;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Obj {
        holder: u64,
        epoch: u64,
        note: String,
    }

    fn obj() -> Obj {
        Obj {
            holder: 7,
            epoch: 3,
            note: "a body long enough to tear in the middle".into(),
        }
    }

    async fn store_with(path: &Path) -> Arc<FaultyStore> {
        let store = FaultyStore::new();
        store
            .inner()
            .put(path, PutPayload::from(serde_json::to_vec(&obj()).unwrap()))
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn a_torn_body_is_re_read() {
        let path = Path::from("leases/p0.json");
        let store = store_with(&path).await;
        store.script(OpKind::Get, "leases", Calls::Nth(1), Fault::Truncated(20));
        let (got, _) = get_json::<Obj>(store.as_ref(), &path)
            .await
            .expect("a torn read must be retried, not reported")
            .expect("object exists");
        assert_eq!(got, obj());
        assert_eq!(store.calls(OpKind::Get, "leases"), 2);
    }

    #[tokio::test]
    async fn a_body_that_never_parses_fails_after_the_retries() {
        let path = Path::from("leases/p0.json");
        let store = store_with(&path).await;
        store.script(OpKind::Get, "leases", Calls::Every, Fault::Truncated(20));
        let started = std::time::Instant::now();
        let err = get_json::<Obj>(store.as_ref(), &path).await.unwrap_err();
        assert!(matches!(err, StoreError::Json(_)), "{err}");
        assert_eq!(
            store.calls(OpKind::Get, "leases"),
            TORN_READ_ATTEMPTS as usize
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the retries must be prompt, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn absent_is_none_and_other_errors_are_not_retried() {
        let path = Path::from("leases/p0.json");
        let store = FaultyStore::new();
        assert!(get_json::<Obj>(store.as_ref(), &path)
            .await
            .unwrap()
            .is_none());
        let store = store_with(&path).await;
        store.script(OpKind::Get, "leases", Calls::Nth(1), Fault::Status(500));
        assert!(get_json::<Obj>(store.as_ref(), &path).await.is_err());
        assert_eq!(store.calls(OpKind::Get, "leases"), 1);
    }
}
