//! Node registry: each mounting node claims a small, cluster-unique
//! integer id with a CAS-create on `nodes/<id>` (DESIGN.md §2). The id
//! scopes ino allocation (`SqliteMeta::set_node_prefix`) and marks log
//! segment origin, so it must never be shared by two live state dirs.

use crate::error::StoreError;
use futures::TryStreamExt;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: u64,
    pub hostname: String,
    pub created_unix: i64,
}

fn node_key(id: u64) -> object_store::path::Path {
    object_store::path::Path::from(format!("nodes/{id:08x}.json"))
}

/// Claim the lowest free node id (>= 1). Retries upward on CAS
/// collision with concurrently joining nodes.
pub async fn claim_node_id(store: Arc<dyn ObjectStore>) -> Result<u64, StoreError> {
    let prefix = object_store::path::Path::from("nodes");
    let taken: Vec<u64> = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|m| {
            let name = m.location.filename()?.strip_suffix(".json")?.to_string();
            u64::from_str_radix(&name, 16).ok()
        })
        .collect();
    let mut candidate = taken.iter().copied().max().unwrap_or(0) + 1;
    loop {
        let info = NodeInfo {
            node_id: candidate,
            hostname: hostname(),
            created_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        };
        let body = serde_json::to_vec(&info)?;
        match store
            .put_opts(
                &node_key(candidate),
                PutPayload::from(body),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => return Ok(candidate),
            Err(object_store::Error::AlreadyExists { .. }) => candidate += 1,
            Err(e) => return Err(e.into()),
        }
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn ids_are_unique_and_monotonic() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let c = claim_node_id(store).await.unwrap();
        assert_eq!((a, b, c), (1, 2, 3));
    }
}
