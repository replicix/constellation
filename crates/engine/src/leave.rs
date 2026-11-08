//! Permanent roster leave (phase 4c).
//!
//! Unmount is a *temporary* departure: the registry record stays, so the
//! node still counts as write-eligible and can block continuation epochs.
//! [`self_leave`] / [`admin_leave`] are the *permanent* departure: a
//! tombstone (`retired: true`) so the numeric id is never recycled, the
//! roster shrinks, and survivors can form epochs without waiting forever.

use crate::designation::DesignationManager;
use crate::epoch::EpochManager;
use constellation_meta::Meta;
use object_store::ObjectStore;
use std::fmt;
use std::sync::Arc;

/// Guard failures shared by self-leave and admin leave. Mapped to
/// control-API error strings by the daemon.
#[derive(Debug)]
pub enum LeaveError {
    OpenEpoch,
    LiveDesignation { node_id: u64, path: String },
    StrandedJournal,
    SelfViaAdminForm,
    LiveLease { node_id: u64, partition: String },
    Other(String),
}

impl fmt::Display for LeaveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenEpoch => write!(
                f,
                "cannot leave while a continuation epoch is open; wait for S3 to return and the epoch to drain"
            ),
            Self::LiveDesignation { node_id, path } => write!(
                f,
                "node {node_id} still holds an offline designation for {path}; run `online` first (or pass --force)"
            ),
            Self::StrandedJournal => write!(
                f,
                "this node was deposed and its deposition recovery has not run yet; run `reintegrate` (or wait for the next sync round) before leaving"
            ),
            Self::SelfViaAdminForm => write!(
                f,
                "refuse leave --node-id for this daemon's own id; omit --node-id for self-leave"
            ),
            Self::LiveLease { node_id, partition } => write!(
                f,
                "node {node_id} still holds a live lease on partition {partition}; wait for release/expiry (or pass --force)"
            ),
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for LeaveError {}

/// Live designations owned by `node_id` (snapshot is already non-released).
pub fn designations_held_by(mgr: &DesignationManager, node_id: u64) -> Vec<String> {
    mgr.snapshot()
        .into_iter()
        .filter(|d| d.designee == node_id)
        .map(|d| d.path)
        .collect()
}

/// Admin form, before its grants are ended: without `force` a live
/// designation of `target` refuses the leave; with it the designations are
/// released, so none is journaled to the node between the end of its grants
/// and its retirement.
pub async fn prepare_admin_leave(
    designations: &DesignationManager,
    target: u64,
    force: bool,
) -> Result<(), LeaveError> {
    if force {
        designations
            .release_of(target)
            .await
            .map_err(|e| LeaveError::Other(format!("releasing designations of {target}: {e}")))?;
    } else if let Some(path) = designations_held_by(designations, target)
        .into_iter()
        .next()
    {
        return Err(LeaveError::LiveDesignation {
            node_id: target,
            path,
        });
    }
    Ok(())
}

/// Admin form: retire a *different* node via a still-mounted peer.
pub async fn admin_leave(
    store: Arc<dyn ObjectStore>,
    designations: &DesignationManager,
    self_id: u64,
    target: u64,
    force: bool,
) -> Result<(), LeaveError> {
    if target == self_id {
        return Err(LeaveError::SelfViaAdminForm);
    }
    if !force {
        if let Some(path) = designations_held_by(designations, target)
            .into_iter()
            .next()
        {
            return Err(LeaveError::LiveDesignation {
                node_id: target,
                path,
            });
        }
        let held = constellation_store_s3::live_leases_held_by(store.clone(), target)
            .await
            .map_err(|e| LeaveError::Other(format!("listing leases: {e}")))?;
        if let Some(partition) = held.into_iter().next() {
            return Err(LeaveError::LiveLease {
                node_id: target,
                partition,
            });
        }
    }
    if force {
        // A designation the node made after `prepare_admin_leave` (before
        // the root marked it leaving) would outlive its retirement: dropped
        // again, from a fresh read, now that its grants are ended.
        designations.refresh().await;
        designations
            .release_of(target)
            .await
            .map_err(|e| LeaveError::Other(format!("releasing designations of {target}: {e}")))?;
    }
    constellation_store_s3::leave_node(store.clone(), target)
        .await
        .map_err(|e| LeaveError::Other(format!("retiring node {target}: {e}")))?;
    // Plan 30 §M10: fence every lease that still names the retired node
    // (epoch bump, expired, its id in `retired`), so it can never renew,
    // flush-re-claim or re-acquire one again — even before it sees its
    // tombstone — and a continuation epoch it carried is abandoned by
    // its members (the flush they wait for will never come).
    let fenced = constellation_store_s3::fence_retired(store, target)
        .await
        .map_err(|e| LeaveError::Other(format!("fencing node {target}'s leases: {e}")))?;
    if !fenced.is_empty() {
        tracing::warn!(node = target, ?fenced, "fenced the retired node's leases");
    }
    Ok(())
}

/// Self-leave, first half: the guards. Callers must already have refused
/// an open epoch. `--force` skips only the designation courtesy check —
/// an open epoch and a stranded journal still refuse. The flush and the
/// release run in the authority core (`Control::Flush`); [`finish_leave`]
/// then retires the record.
pub fn pre_leave_checks(
    meta: &Meta,
    designations: &DesignationManager,
    node_id: u64,
    force: bool,
) -> Result<(), LeaveError> {
    if matches!(
        meta.kv_get("lease_lost").ok().flatten().as_deref(),
        Some("1")
    ) {
        let stranded = meta.unmarked_journal_len().map(|n| n > 0).unwrap_or(true);
        if stranded {
            return Err(LeaveError::StrandedJournal);
        }
    }
    if !force {
        if let Some(path) = designations_held_by(designations, node_id)
            .into_iter()
            .next()
        {
            return Err(LeaveError::LiveDesignation { node_id, path });
        }
    }
    Ok(())
}

/// Self-leave, second half (after the core flushed and released):
/// tombstone the registry record and mark the state dir spent.
pub async fn finish_leave(
    store: Arc<dyn ObjectStore>,
    meta: &Meta,
    node_id: u64,
) -> Result<(), LeaveError> {
    constellation_store_s3::leave_node(store, node_id)
        .await
        .map_err(|e| LeaveError::Other(format!("retiring node {node_id}: {e}")))?;
    meta.kv_set("left", "1")
        .map_err(|e| LeaveError::Other(format!("persisting left flag: {e}")))?;
    Ok(())
}

/// Whether a self-leave's retirement landed although `finish_leave` did not
/// report it (a timeout after the registry's PUT, an answer lost): the
/// registry names the node retired. Then the leave is done, and the same
/// flag `finish_leave` persists is persisted here; otherwise nothing
/// changed and the leave failed.
pub async fn complete_if_retired(
    store: Arc<dyn ObjectStore>,
    meta: &Meta,
    node_id: u64,
) -> Result<bool, LeaveError> {
    let record = constellation_store_s3::get_node(store, node_id)
        .await
        .map_err(|e| LeaveError::Other(format!("reading node {node_id}'s record: {e}")))?;
    if !record.is_some_and(|r| r.retired) {
        return Ok(false);
    }
    meta.kv_set("left", "1")
        .map_err(|e| LeaveError::Other(format!("persisting left flag: {e}")))?;
    Ok(true)
}

/// Refuse self-leave while an epoch promise is open (not forceable).
pub fn refuse_open_epoch(epochs: &EpochManager) -> Result<(), LeaveError> {
    if epochs.is_open() {
        Err(LeaveError::OpenEpoch)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_authority::Now;
    use constellation_meta::MetaStore;
    use constellation_net::Peers;
    use constellation_store_s3::designation::{DesignationMode, DesignationStore};
    use constellation_store_s3::{
        claim_node_id, get_node, write_eligible_roster, Lease, LeaseMode, LeaseStore,
    };
    use object_store::memory::InMemory;

    fn open_meta() -> Arc<Meta> {
        Arc::new(Meta::open_in_memory().unwrap())
    }

    #[tokio::test]
    async fn self_leave_tombstones_and_shrinks_roster() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        assert_eq!(write_eligible_roster(store.clone()).await.unwrap(), [a, b]);

        let meta = open_meta();
        meta.set_node_prefix(b).unwrap();
        let designations = DesignationManager::new(
            DesignationStore::new(store.clone(), DesignationMode::Cas),
            meta.clone(),
            Peers::disabled(),
            b,
        );
        pre_leave_checks(&meta, &designations, b, false).unwrap();
        finish_leave(store.clone(), &meta, b).await.unwrap();

        assert_eq!(write_eligible_roster(store.clone()).await.unwrap(), [a]);
        assert!(get_node(store, b).await.unwrap().unwrap().retired);
        assert_eq!(meta.kv_get("left").unwrap().as_deref(), Some("1"));
    }

    /// A retirement that landed without being reported (the registry's PUT
    /// went through, the answer or the caller's bound did not) completes the
    /// leave: the node is retired in the registry and the left flag is
    /// persisted. A node the registry still lists live is not.
    #[tokio::test]
    async fn a_retirement_that_landed_unreported_completes_the_leave() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let meta = open_meta();
        assert!(!complete_if_retired(store.clone(), &meta, b).await.unwrap());
        assert_eq!(meta.kv_get("left").unwrap(), None);
        // The PUT lands; `finish_leave` never reports it.
        constellation_store_s3::leave_node(store.clone(), b)
            .await
            .unwrap();
        assert_eq!(meta.kv_get("left").unwrap(), None, "not persisted yet");
        assert!(complete_if_retired(store.clone(), &meta, b).await.unwrap());
        assert_eq!(meta.kv_get("left").unwrap().as_deref(), Some("1"));
        // (Another node's record is untouched.)
        assert!(!get_node(store, a).await.unwrap().unwrap().retired);
    }

    #[tokio::test]
    async fn admin_leave_refuses_self() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = claim_node_id(store.clone()).await.unwrap();
        let meta = open_meta();
        let designations = DesignationManager::new(
            DesignationStore::new(store.clone(), DesignationMode::Cas),
            meta,
            Peers::disabled(),
            id,
        );
        let err = admin_leave(store, &designations, id, id, false)
            .await
            .unwrap_err();
        assert!(matches!(err, LeaveError::SelfViaAdminForm));
    }

    #[tokio::test]
    async fn admin_leave_refuses_live_lease_without_force() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let ls = LeaseStore::new(store.clone(), "p0", LeaseMode::Cas);
        ls.try_create(&Lease::granted("p0", b, 1, 60_000))
            .await
            .unwrap();
        let meta = open_meta();
        let designations = DesignationManager::new(
            DesignationStore::new(store.clone(), DesignationMode::Cas),
            meta,
            Peers::disabled(),
            a,
        );
        let err = admin_leave(store.clone(), &designations, a, b, false)
            .await
            .unwrap_err();
        assert!(matches!(err, LeaveError::LiveLease { .. }), "{err}");
        admin_leave(store.clone(), &designations, a, b, true)
            .await
            .unwrap();
        assert!(get_node(store, b).await.unwrap().unwrap().retired);
    }

    /// Plan 30 §M10: an admin leave fences the retired node's lease — the
    /// epoch bumps, it expires, the node joins `retired` — so the retired
    /// node's own renewal (a CAS on the old tag) loses, and its claim of
    /// the fenced object is refused however it reads it; another node's
    /// claim is an ordinary takeover with an epoch marker.
    #[tokio::test]
    async fn admin_leave_fences_the_retired_nodes_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let ls = LeaseStore::new(store.clone(), "p0", LeaseMode::Cas);
        let tag_b = ls
            .try_create(&Lease::granted("p0", b, 3, 60_000))
            .await
            .unwrap();
        let meta = open_meta();
        let designations = DesignationManager::new(
            DesignationStore::new(store.clone(), DesignationMode::Cas),
            meta,
            Peers::disabled(),
            a,
        );
        admin_leave(store.clone(), &designations, a, b, true)
            .await
            .unwrap();
        let (fenced, tag) = ls.get().await.unwrap().unwrap();
        assert_eq!(fenced.holder, b);
        assert_eq!(fenced.epoch, 4, "the epoch bumps");
        assert!(
            !fenced.released,
            "not released: the next taker ships a marker"
        );
        assert!(fenced.is_expired(constellation_store_s3::lease::now_unix_ms()));
        assert_eq!(fenced.retired, vec![b]);
        // The retired node's renewal on its old tag loses.
        let renewed = Lease::granted("p0", b, 3, 60_000);
        assert!(ls.try_swap(&renewed, &tag_b).await.is_err());
        // Its core refuses the fenced object; another node's classifies
        // it as a takeover with a marker.
        let now = constellation_authority::Ms(constellation_store_s3::lease::now_unix_ms());
        let as_b = constellation_authority::Config::defaults(b, 1);
        let plan = constellation_authority::core::LeaseState::default().classify(
            Now::same(now.0),
            &as_b,
            Some((fenced.clone(), tag.clone())),
            0,
        );
        assert!(
            matches!(plan, constellation_authority::core::Plan::Refused(_)),
            "{plan:?}"
        );
        let as_a = constellation_authority::Config::defaults(a, 1);
        let plan = constellation_authority::core::LeaseState::default().classify(
            Now::same(now.0),
            &as_a,
            Some((fenced, tag)),
            0,
        );
        assert!(
            matches!(
                plan,
                constellation_authority::core::Plan::Claim {
                    takeover: true,
                    marker: true,
                    ..
                }
            ),
            "{plan:?}"
        );
        // A second leave is a no-op on the lease.
        admin_leave(store.clone(), &designations, a, b, true)
            .await
            .unwrap();
        assert_eq!(ls.get().await.unwrap().unwrap().0.epoch, 4);
    }

    #[tokio::test]
    async fn self_leave_refuses_stranded_deposition() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = claim_node_id(store.clone()).await.unwrap();
        let meta = open_meta();
        meta.set_node_prefix(id).unwrap();
        meta.kv_set("lease_lost", "1").unwrap();
        MetaStore::mkdir(
            &*meta,
            constellation_fs_core::types::ROOT_INO,
            "pending",
            0o755,
            0,
            0,
        )
        .unwrap();
        let designations = DesignationManager::new(
            DesignationStore::new(store.clone(), DesignationMode::Cas),
            meta.clone(),
            Peers::disabled(),
            id,
        );
        let err = pre_leave_checks(&meta, &designations, id, true).unwrap_err();
        assert!(matches!(err, LeaveError::StrandedJournal), "{err}");
    }

    /// Review MUST-2: an admin leave drops the designations of the node
    /// before its grants are ended (`--force`), or refuses before anything is
    /// ended (without it), so none is journaled to the node meanwhile.
    #[tokio::test]
    async fn admin_leave_drops_or_refuses_the_nodes_designations_before_ending_its_grants() {
        use constellation_store_s3::designation::Designation;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let ds = DesignationStore::new(store.clone(), DesignationMode::Cas);
        ds.create(&Designation::new("/x", b, false)).await.unwrap();
        let designations = DesignationManager::new(ds, open_meta(), Peers::disabled(), a);
        designations.refresh().await;
        assert_eq!(
            designations_held_by(&designations, b),
            vec!["/x".to_string()]
        );
        let err = prepare_admin_leave(&designations, b, false)
            .await
            .unwrap_err();
        assert!(matches!(err, LeaveError::LiveDesignation { .. }), "{err}");
        assert_eq!(designations_held_by(&designations, b).len(), 1);
        prepare_admin_leave(&designations, b, true).await.unwrap();
        assert!(designations_held_by(&designations, b).is_empty());
    }

    /// A designation the node makes between `prepare_admin_leave` and the
    /// retirement is dropped by the forced leave too (it would outlive the
    /// registry entry).
    #[tokio::test]
    async fn a_designation_made_after_the_prepare_does_not_outlive_a_forced_leave() {
        use constellation_store_s3::designation::Designation;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let ds = DesignationStore::new(store.clone(), DesignationMode::Cas);
        let designations = DesignationManager::new(ds, open_meta(), Peers::disabled(), a);
        designations.refresh().await;
        prepare_admin_leave(&designations, b, true).await.unwrap();
        // The node designates a path after that, through the store.
        let ds = DesignationStore::new(store.clone(), DesignationMode::Cas);
        ds.create(&Designation::new("/late", b, false))
            .await
            .unwrap();
        admin_leave(store.clone(), &designations, a, b, true)
            .await
            .unwrap();
        designations.refresh().await;
        assert!(
            designations_held_by(&designations, b).is_empty(),
            "a designation outlived the retirement"
        );
    }

    #[test]
    fn open_epoch_is_not_forceable() {
        let meta = open_meta();
        let epochs = EpochManager::new(1, meta, Peers::disabled());
        refuse_open_epoch(&epochs).unwrap();
    }
}
