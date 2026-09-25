//! Offline designation (DESIGN.md §5.2): the daemon-side manager tying
//! together the S3 `DesignationStore` and, since plan 30 §M11 phase 2b,
//! the delegation table.
//!
//! A designation *is* a delegation now (PROGRESS "Plan 30 M11 — phase
//! 1", "Designations as non-stealable delegations"): the root writes
//! `Delegate { dir, node: designee, gen, designated: true }` when it
//! sees the designation object (`SyncDesignations`, after every refresh
//! here), never recalls it by TTL or placement, refuses a cross-subtree
//! op touching it (`EXDEV`) and any write under it that reaches the root
//! (`EROFS`: only the designee sequences it), and writes the `Recall`
//! when `online` releases the object. A non-designee writing under the
//! path forwards to the designee like any delegated write; the designee's
//! own writes run on its fast path under a grant that never expires
//! (DESIGN.md §5.2: the designee writes while isolated). The M3-era P2P
//! grant/renew/flush-ack machinery is gone.
//!
//! `--ro` designations stay a pinning/read guarantee and delegate
//! nothing.

use anyhow::{bail, Context, Result};
use constellation_meta::Meta;
use constellation_net::Peers;
use constellation_store_s3::designation::{Designation, DesignationStore};
use std::sync::{Arc, Mutex};

/// Daemon-side manager tying together the S3 `DesignationStore` and the
/// delegation table.
pub struct DesignationManager {
    store: DesignationStore,
    meta: Arc<Meta>,
    node_id: u64,
    /// Cached snapshot of every live (non-released) designation,
    /// refreshed on demand and by the periodic sync loop.
    active: Mutex<Vec<Designation>>,
}

impl DesignationManager {
    pub fn new(store: DesignationStore, meta: Arc<Meta>, _peers: Peers, node_id: u64) -> Self {
        Self {
            store,
            meta,
            node_id,
            active: Mutex::new(Vec::new()),
        }
    }

    /// Re-read every live designation from S3 into the local cache. Cheap
    /// to call periodically (one LIST + N small GETs, all rare operator
    /// objects).
    pub async fn refresh(&self) {
        match self.store.list_all().await {
            Ok(all) => {
                let live: Vec<Designation> = all.into_iter().filter(|d| !d.released).collect();
                *self.active.lock().unwrap() = live;
            }
            Err(e) => {
                tracing::debug!(error = %e, "designation refresh failed; keeping cached view")
            }
        }
    }

    /// Every currently-active designation, for the control API.
    pub fn snapshot(&self) -> Vec<Designation> {
        self.active.lock().unwrap().clone()
    }

    /// Plan 30 §M11 phase 2b: the write designations as `(dir, designee)`
    /// for the root's table (`--ro` ones delegate nothing; a path that
    /// does not resolve yet is skipped until it does).
    pub fn delegation_entries(&self) -> Vec<(u64, u64)> {
        let live = self.snapshot();
        live.iter()
            .filter(|d| !d.read_only)
            .filter_map(|d| {
                self.meta
                    .resolve_path(&d.path)
                    .ok()
                    .flatten()
                    .map(|ino| (ino, d.designee))
            })
            .collect()
    }

    pub async fn offline(&self, path: &str, read_only: bool) -> Result<String> {
        self.meta
            .resolve_path(path)
            .with_context(|| format!("resolving {path}"))?
            .ok_or_else(|| anyhow::anyhow!("no such path: {path}"))?;
        let d = Designation::new(path, self.node_id, read_only);
        self.store
            .create(&d)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        self.refresh().await;
        Ok(format!(
            "designated {path} (node {}{})",
            self.node_id,
            if read_only { ", read-only" } else { "" }
        ))
    }

    /// `constellation online <path>`: release a designation. Only the
    /// designee may release its own claim — DESIGN.md's "non-stealable"
    /// property extends to ending it, otherwise a network partition
    /// would let another node unilaterally strip the designee's
    /// authority out from under an in-flight write.
    pub async fn online(&self, path: &str) -> Result<String> {
        let (d, tag) = self
            .store
            .get(path)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("no designation at {path}"))?;
        if d.released {
            bail!("{path} is already online");
        }
        if d.designee != self.node_id {
            bail!(
                "{path} is designated to node {}, not this node ({}): \
                 only the designee can release it",
                d.designee,
                self.node_id
            );
        }
        self.store
            .release(&d, &tag)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        self.refresh().await;
        Ok(format!("{path} back online"))
    }
}
