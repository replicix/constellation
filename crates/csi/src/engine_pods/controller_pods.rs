//! The controller-owned engine pods' lifecycle across controller replicas
//! (37-k6b): holds and retiring marks, the pool records, and the purge
//! worker's backend ([`crate::purge::PurgeBackend`]).
//!
//! **Retiring a controller-owned pod.** A pod is deleted on purpose in
//! three cases: the purge worker reaps it (its pool is empty), rolls it
//! (its engine settings drifted), or a delete stops the pod it started
//! only for itself ([`Engines::retire`]). The controller runs `replicas: 2`
//! and the sidecars elect their leaders independently of the purge lease,
//! so the replica that retires a pod may not be the one whose RPC is in
//! the middle of using it; an in-process lock cannot see the other. So:
//!
//! - Every RPC that reaches a pod first **holds** it: its replica writes
//!   `constellation.dev/held-by-<10 hex of the replica's name>` = the unix
//!   ms until which it holds the pod ([`HOLD_FOR`] ahead) with a
//!   compare-and-swap on the pod's `resourceVersion`, on the first use
//!   after none in that replica, and renews it while any RPC of the
//!   replica still uses the pod. A hold write also clears a stale
//!   retiring mark.
//! - A retire first **marks** the pod: `constellation.dev/retiring` =
//!   `<its replica's key> <unix ms>`, by a compare-and-swap too, and only
//!   when no other replica's hold is live (and no RPC of its own uses
//!   the pod). Only then does it look again (a reap: is the pool still
//!   empty?), renew the mark (the same swap, only while the mark is still
//!   its own and fresh by its own clock and no other hold is live), leave
//!   the registry, and delete with that `resourceVersion` as the
//!   precondition: a hold written meanwhile fails the delete.
//! - A hold that finds a fresh mark does not write: the RPC waits (bounded
//!   by the ready timeout) for the pod to go — and starts it again, or
//!   for the mark to be lifted.
//!
//! Both writes are compare-and-swaps on the same object, so they
//! serialize: either the hold landed first (the mark's own swap then fails,
//! or the retire sees the live hold and gives up), or the mark did (the
//! hold's swap fails, re-reads, and waits). A replica that dies leaves a
//! hold that expires ([`HOLD_FOR`], plus [`HOLD_SKEW`] for clocks), or a
//! mark that does ([`RETIRING_TTL`], plus [`HOLD_SKEW`] for the others;
//! its own replica trusts it for the TTL only).
//!
//! **Pool records** (the `constellation-csi-pools` ConfigMap): which pools
//! may have trash, so the purge worker can bring a pool's pod back after
//! it went with nobody asking for it (module docs of [`crate::purge`]).

use super::*;
use crate::purge::{PurgeBackend, PurgePool};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};

/// A controller replica's hold on a controller-owned pod (module docs):
/// `<prefix><10 hex>` = unix ms.
pub const ANNOTATION_HELD_PREFIX: &str = "constellation.dev/held-by-";
/// A retire in progress (module docs): `<holder key> <unix ms>`.
pub const ANNOTATION_RETIRING: &str = "constellation.dev/retiring";
/// How far ahead a hold is written.
pub const HOLD_FOR: Duration = Duration::from_secs(120);
/// A hold with less than this left is renewed (while used).
const HOLD_RENEW_BELOW: Duration = Duration::from_secs(60);
/// The clock skew between replicas a hold's expiry tolerates.
pub const HOLD_SKEW: Duration = Duration::from_secs(30);
/// A retiring mark older than this is stale: its replica died mid-retire.
pub const RETIRING_TTL: Duration = Duration::from_secs(120);
/// A recorded pool no PV names is given up on after failing to come up in
/// this many purge passes in a row ...
pub const GIVE_UP_PASSES: u32 = 3;
/// ... spanning at least this long (a bucket outage is waited out).
pub const GIVE_UP_AFTER: Duration = Duration::from_secs(3600);
/// How many pools one purge pass brings up at once.
const BRINGUP_CONCURRENCY: usize = 8;
/// The pool records' ConfigMap, in the driver's namespace.
pub const POOLS_CONFIGMAP: &str = "constellation-csi-pools";

/// Replica `identity`'s hold annotation.
pub fn hold_key(identity: &str) -> String {
    let hash = blake3::hash(identity.as_bytes()).to_hex();
    format!("{ANNOTATION_HELD_PREFIX}{}", &hash[..10])
}

/// The holds on `pod` of replicas other than `me` (a [`hold_key`]) still
/// live at `now_ms`; an unreadable one counts as live.
pub fn live_holds(pod: &Pod, me: &str, now_ms: u64) -> Vec<String> {
    let skew = HOLD_SKEW.as_millis() as u64;
    pod.metadata
        .annotations
        .iter()
        .flatten()
        .filter(|(k, _)| k.starts_with(ANNOTATION_HELD_PREFIX) && k.as_str() != me)
        .filter(|(_, v)| {
            v.parse::<u64>()
                .map_or(true, |until| until.saturating_add(skew) > now_ms)
        })
        .map(|(k, _)| k.clone())
        .collect()
}

/// `pod`'s retiring mark: who, and when (unix ms by its clock).
fn retiring_mark(pod: &Pod) -> Option<(String, u64)> {
    let mark = pod
        .metadata
        .annotations
        .as_ref()?
        .get(ANNOTATION_RETIRING)?;
    let (who, at) = mark.split_once(' ')?;
    Some((who.to_string(), at.parse().ok()?))
}

/// Who marked `pod` retiring, if the mark may still be fresh at `now_ms`
/// by any replica's clock ([`HOLD_SKEW`] allowance): until then nobody
/// takes the pod over from the marking replica.
pub fn retiring_by(pod: &Pod, now_ms: u64) -> Option<String> {
    let (who, at) = retiring_mark(pod)?;
    let until = at
        .saturating_add(RETIRING_TTL.as_millis() as u64)
        .saturating_add(HOLD_SKEW.as_millis() as u64);
    (now_ms < until).then_some(who)
}

/// Whether `pod` carries replica `me`'s own mark, fresh by its own clock
/// at `now_ms` (no skew allowance: within it, no other replica has seen
/// the mark go stale).
pub fn own_mark_fresh(pod: &Pod, me: &str, now_ms: u64) -> bool {
    retiring_mark(pod).is_some_and(|(who, at)| {
        who == me && now_ms < at.saturating_add(RETIRING_TTL.as_millis() as u64)
    })
}

/// `pod`'s annotations without any hold or retiring mark (a pod made
/// from another's spec starts unheld).
pub fn without_holds(
    annotations: Option<BTreeMap<String, String>>,
) -> Option<BTreeMap<String, String>> {
    annotations.map(|mut a| {
        a.retain(|k, _| !k.starts_with(ANNOTATION_HELD_PREFIX) && k != ANNOTATION_RETIRING);
        a
    })
}

fn now_ms() -> u64 {
    unix_ms() as u64
}

/// This replica's uses of one pod.
#[derive(Debug, Default)]
pub(super) struct HoldState {
    /// RPCs using it now.
    count: usize,
    /// Until when its hold annotation was last written (unix ms).
    until_ms: u64,
}

/// One RPC's use of a pod (module docs); released when dropped.
pub(super) struct Hold {
    manager: std::sync::Weak<EnginePodManager>,
    name: String,
}

impl Drop for Hold {
    fn drop(&mut self) {
        if let Some(m) = self.manager.upgrade() {
            if let Some(h) = m.holds.lock().unwrap().get_mut(&self.name) {
                h.count = h.count.saturating_sub(1);
            }
        }
    }
}

pub(super) enum Held {
    Yes(Hold),
    /// Marked retiring by another replica (or terminating): wait.
    Retiring,
}

/// A pool that may have trash (module docs), enough to bring its
/// controller-owned pod back: the class parameters its volume was
/// provisioned with and the provisioner secret that unlocks it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolRecord {
    pub fs_uuid: String,
    pub shard: u32,
    pub parameters: BTreeMap<String, String>,
    /// `(namespace, name)`.
    pub secret: Option<(String, String)>,
}

/// Where external-provisioner keeps the provisioner secret a PV's delete
/// needs, so the delete works after its class is gone.
pub const ANNOTATION_DELETION_SECRET_NAME: &str =
    "volume.kubernetes.io/provisioner-deletion-secret-name";
pub const ANNOTATION_DELETION_SECRET_NAMESPACE: &str =
    "volume.kubernetes.io/provisioner-deletion-secret-namespace";

/// `(fs uuid, shard)` of `pv` when it is a pool volume of this driver.
pub fn pool_volume_of(pv: &PersistentVolume) -> Option<(String, u32)> {
    let csi = pv.spec.as_ref()?.csi.as_ref()?;
    if csi.driver != crate::identity::DRIVER_NAME {
        return None;
    }
    match VolumeId::parse(&csi.volume_handle).ok()? {
        VolumeId::Pool { fs_uuid, shard, .. } => Some((fs_uuid, shard)),
        _ => None,
    }
}

impl PoolRecord {
    /// The record of pool volume `pv` (see [`pool_volume_of`]), from the PV
    /// itself so a deleted class loses nothing: the parameters its
    /// `volumeAttributes` carry ([`crate::params::volume_context`]) and the
    /// deletion secret its annotations name. `class` (its StorageClass, if
    /// it still exists) fills in only what the PV lacks. `Err`: why no
    /// pool can be rebuilt from it.
    pub fn from_pv(
        pv: &PersistentVolume,
        class: Option<&StorageClass>,
    ) -> Result<PoolRecord, String> {
        let (fs_uuid, shard) = pool_volume_of(pv).ok_or("not a pool volume of this driver")?;
        // Class parameters only: no CO keys (`csi.storage.k8s.io/*`,
        // external-provisioner's `storage.kubernetes.io/csiProvisionerIdentity`).
        let own = |m: &BTreeMap<String, String>| -> BTreeMap<String, String> {
            m.iter()
                .filter(|(k, _)| !k.contains('/') && k.as_str() != crate::params::SHARD_KEY)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let attributes = pv
            .spec
            .as_ref()
            .and_then(|s| s.csi.as_ref())
            .and_then(|c| c.volume_attributes.as_ref())
            .map(own)
            .unwrap_or_default();
        let class_params = class.and_then(|c| c.parameters.clone()).unwrap_or_default();
        let parameters = if !attributes.is_empty() {
            attributes
        } else if class.is_some() {
            own(&class_params)
        } else {
            return Err("its volumeAttributes are empty and its StorageClass is gone".into());
        };
        let annotations = pv.metadata.annotations.as_ref();
        let annotated = |k: &str| {
            annotations
                .and_then(|a| a.get(k))
                .filter(|v| !v.is_empty())
                .cloned()
        };
        let secret = match (
            annotated(ANNOTATION_DELETION_SECRET_NAMESPACE),
            annotated(ANNOTATION_DELETION_SECRET_NAME),
        ) {
            (Some(namespace), Some(name)) => Some((namespace, name)),
            _ => provisioner_secret_ref(&class_params, pv),
        };
        let rec = PoolRecord {
            fs_uuid,
            shard,
            parameters,
            secret,
        };
        rec.pool()?;
        Ok(rec)
    }

    /// The pool (without secrets) and its controller-owned pod's name.
    pub fn pool(&self) -> Result<(PoolRef, String), String> {
        let class = ClassParams::parse(&self.parameters.clone().into_iter().collect())?;
        if self.shard >= class.shards {
            return Err(format!(
                "the class has {} shard(s); the record names shard {}",
                class.shards, self.shard
            ));
        }
        let pool = PoolRef {
            class,
            shard: self.shard,
            secrets: BTreeMap::new(),
        };
        let name = pod_name(&pool);
        Ok((pool, name))
    }
}

/// `POOLS_CONFIGMAP`'s data: pod name → record. Unreadable entries are
/// logged and skipped.
pub fn parse_records(data: &BTreeMap<String, String>) -> BTreeMap<String, PoolRecord> {
    data.iter()
        .filter_map(|(pod, json)| match serde_json::from_str(json) {
            Ok(rec) => Some((pod.clone(), rec)),
            Err(e) => {
                tracing::warn!(pod, error = %e, "an unreadable pool record; skipped");
                None
            }
        })
        .collect()
}

impl EnginePodManager {
    /// How many RPCs of this process use pod `name` now.
    pub(super) fn held_here(&self, name: &str) -> usize {
        self.holds.lock().unwrap().get(name).map_or(0, |h| h.count)
    }

    /// Hold pod `name` (module docs), `pod` as last read. The caller holds
    /// its bring-up lock.
    pub(super) async fn hold(&self, name: &str, pod: &Pod) -> Result<Held, ControlError> {
        let held = || Hold {
            manager: self.this.clone(),
            name: name.to_string(),
        };
        {
            // Held by another RPC of this process: its hold is live (and
            // renewed), so no mark can have been set since — unless this
            // incarnation does not carry it (the pod was recreated under
            // the RPC): then it is written again below.
            let carries = pod
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(&self.hold_key));
            let mut holds = self.holds.lock().unwrap();
            let h = holds.entry(name.to_string()).or_default();
            if h.count > 0 && carries {
                h.count += 1;
                return Ok(Held::Yes(held()));
            }
        }
        let mut pod = pod.clone();
        for _ in 0..8 {
            let now = now_ms();
            if pod.metadata.deletion_timestamp.is_some() {
                return Ok(Held::Retiring);
            }
            if retiring_by(&pod, now).is_some_and(|who| who != self.hold_key) {
                return Ok(Held::Retiring);
            }
            let until = now + HOLD_FOR.as_millis() as u64;
            let patch = serde_json::json!({"metadata": {
                "resourceVersion": pod.metadata.resource_version,
                "annotations": {
                    self.hold_key.as_str(): until.to_string(),
                    ANNOTATION_RETIRING: null,
                },
            }});
            match self
                .pods
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(written) if written.metadata.deletion_timestamp.is_some() => {
                    return Ok(Held::Retiring)
                }
                Ok(_) => {
                    let mut holds = self.holds.lock().unwrap();
                    let h = holds.entry(name.to_string()).or_default();
                    h.count += 1;
                    h.until_ms = until;
                    return Ok(Held::Yes(held()));
                }
                Err(e) if is_status(&e, 409) => {
                    pod = match self.pods.get_opt(name).await {
                        Ok(Some(pod)) => pod,
                        Ok(None) => return Ok(Held::Retiring),
                        Err(e) => return Err(kube_err("reading the engine pod", e)),
                    };
                }
                Err(e) if is_status(&e, 404) => return Ok(Held::Retiring),
                Err(e) => return Err(kube_err("holding the engine pod", e)),
            }
        }
        Err(ControlError::unavailable(format!(
            "engine pod {name} keeps changing; could not hold it"
        )))
    }

    /// Renew this process's live holds that run low (module docs), every
    /// 15 s while it runs.
    pub(super) fn spawn_hold_renewal(this: std::sync::Weak<EnginePodManager>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                let Some(m) = this.upgrade() else { return };
                let now = now_ms();
                let due: Vec<String> = m
                    .holds
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, h)| {
                        h.count > 0 && h.until_ms < now + HOLD_RENEW_BELOW.as_millis() as u64
                    })
                    .map(|(name, _)| name.clone())
                    .collect();
                for name in due {
                    let until = now_ms() + HOLD_FOR.as_millis() as u64;
                    let patch = serde_json::json!({"metadata": {"annotations": {
                        m.hold_key.as_str(): until.to_string(),
                    }}});
                    match m
                        .pods
                        .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await
                    {
                        Ok(_) => {
                            if let Some(h) = m.holds.lock().unwrap().get_mut(&name) {
                                h.until_ms = until;
                            }
                        }
                        Err(e) if is_status(&e, 404) => {}
                        Err(e) => tracing::warn!(pod = %name, error = %e,
                            "renewing this replica's hold on an engine pod"),
                    }
                }
            }
        });
    }

    /// Mark pod `name` retiring (module docs) unless an RPC of this or
    /// any other replica holds it: the pod as marked. The caller holds its
    /// bring-up lock.
    async fn mark_retiring(&self, name: &str) -> Result<Option<Pod>, ControlError> {
        if self.held_here(name) > 0 || self.relay_in_use(name) {
            tracing::debug!(pod = name, "engine pod in use here; not retiring it");
            return Ok(None);
        }
        let Some(pod) = self
            .pods
            .get_opt(name)
            .await
            .map_err(|e| kube_err("reading the engine pod", e))?
        else {
            return Ok(None);
        };
        if pod.metadata.deletion_timestamp.is_some() {
            return Ok(None);
        }
        let now = now_ms();
        let holds = live_holds(&pod, &self.hold_key, now);
        if !holds.is_empty() {
            tracing::info!(
                pod = name,
                ?holds,
                "engine pod held by another controller replica; not retiring it now"
            );
            return Ok(None);
        }
        if retiring_by(&pod, now).is_some_and(|who| who != self.hold_key) {
            return Ok(None);
        }
        let patch = serde_json::json!({"metadata": {
            "resourceVersion": pod.metadata.resource_version,
            "annotations": {ANNOTATION_RETIRING: format!("{} {now}", self.hold_key)},
        }});
        match self
            .pods
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(marked) => Ok(Some(marked)),
            // Somebody wrote the pod meanwhile (a hold, most likely).
            Err(e) if is_status(&e, 409) || is_status(&e, 404) => Ok(None),
            Err(e) => Err(kube_err("marking the engine pod retiring", e)),
        }
    }

    /// Lift this replica's retiring mark from `name` (best effort: a mark
    /// left behind goes stale).
    async fn unmark(&self, name: &str) {
        let patch = serde_json::json!({"metadata": {"annotations": {ANNOTATION_RETIRING: null}}});
        if let Err(e) = self
            .pods
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            if !is_status(&e, 404) {
                tracing::info!(pod = name, error = %e, "lifting a retiring mark; it goes stale");
            }
        }
    }

    /// Whether a client of this process into `name`'s relay is alive.
    fn relay_in_use(&self, name: &str) -> bool {
        self.relays
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|r| Arc::strong_count(r) > 1)
    }

    /// The relay into `name`'s incarnation, taken out of the cache, when no
    /// RPC holds a client into it (`Err`: in use). The caller holds the
    /// bring-up lock.
    fn take_idle_relay(&self, name: &str) -> Result<Option<Arc<Relay>>, ()> {
        let mut relays = self.relays.lock().unwrap();
        if let Some(relay) = relays.get(name) {
            if Arc::strong_count(relay) > 1 {
                return Err(());
            }
        }
        Ok(relays.remove(name))
    }

    /// Wait (bounded by the ready timeout) for pod `name` to be gone.
    async fn wait_deleted(&self, name: &str) -> Result<(), ControlError> {
        let deadline = Instant::now() + self.cfg.ready_timeout;
        loop {
            match self.pods.get_opt(name).await {
                Ok(None) => return Ok(()),
                Ok(Some(_)) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(500)).await
                }
                Ok(Some(_)) => {
                    return Err(ControlError::unavailable(format!(
                        "engine pod {name} is still being deleted"
                    )))
                }
                Err(e) => return Err(kube_err("reading the engine pod", e)),
            }
        }
    }

    /// Renew this replica's retiring mark on `name` (a compare-and-swap):
    /// the pod as renewed, or `None` when the mark is no longer this
    /// replica's fresh one or another replica holds the pod (it took the
    /// pod over once the mark went stale: the retire must not go on).
    async fn renew_mark(&self, name: &str) -> Result<Option<Pod>, ControlError> {
        for _ in 0..4 {
            let Some(pod) = self
                .pods
                .get_opt(name)
                .await
                .map_err(|e| kube_err("reading the engine pod", e))?
            else {
                return Ok(None);
            };
            let now = now_ms();
            if pod.metadata.deletion_timestamp.is_some()
                || !own_mark_fresh(&pod, &self.hold_key, now)
                || !live_holds(&pod, &self.hold_key, now).is_empty()
            {
                return Ok(None);
            }
            let patch = serde_json::json!({"metadata": {
                "resourceVersion": pod.metadata.resource_version,
                "annotations": {ANNOTATION_RETIRING: format!("{} {now}", self.hold_key)},
            }});
            match self
                .pods
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(renewed) => return Ok(Some(renewed)),
                Err(e) if is_status(&e, 409) => continue,
                Err(e) if is_status(&e, 404) => return Ok(None),
                Err(e) => return Err(kube_err("renewing the retiring mark", e)),
            }
        }
        Ok(None)
    }

    /// Delete pod `name` only as `fenced` (read with this replica's fresh
    /// mark and no other live hold): with that `resourceVersion` as the
    /// precondition, and on a conflict only after a re-read still shows
    /// the same (a status update, say). Whether it went.
    async fn delete_fenced(&self, name: &str, mut fenced: Pod) -> Result<bool, ControlError> {
        for _ in 0..4 {
            let params = DeleteParams {
                preconditions: Some(Preconditions {
                    resource_version: fenced.metadata.resource_version.clone(),
                    uid: fenced.metadata.uid.clone(),
                }),
                ..Default::default()
            };
            match self.pods.delete(name, &params).await {
                Ok(_) => return Ok(true),
                Err(e) if is_status(&e, 404) => return Ok(true),
                Err(e) if is_status(&e, 409) => {
                    let Some(pod) = self
                        .pods
                        .get_opt(name)
                        .await
                        .map_err(|e| kube_err("reading the engine pod", e))?
                    else {
                        return Ok(true);
                    };
                    let now = now_ms();
                    if pod.metadata.uid != fenced.metadata.uid {
                        // Another incarnation: not the one this retire is for.
                        return Ok(true);
                    }
                    if pod.metadata.deletion_timestamp.is_some() {
                        return Ok(true);
                    }
                    if !own_mark_fresh(&pod, &self.hold_key, now)
                        || !live_holds(&pod, &self.hold_key, now).is_empty()
                    {
                        tracing::warn!(
                            pod = name,
                            "another controller replica took the engine pod over while it was \
                             being retired; kept (its engine may have left the registry: the \
                             next retire of it deletes it)"
                        );
                        return Ok(false);
                    }
                    fenced = pod;
                }
                Err(e) => return Err(kube_err("deleting the engine pod", e)),
            }
        }
        Err(ControlError::unavailable(format!(
            "engine pod {name} keeps changing; not deleted now"
        )))
    }

    /// Leave the registry through this process's relay into `name` (if
    /// any, else `pod`'s, attached for it), then delete the pod: whether it
    /// did (`false`: a client of this process uses the relay after all —
    /// the mark is lifted and nothing happens —, or the mark was lost to
    /// another replica's hold). The mark is renewed just before the leave
    /// and the delete is fenced by it ([`Self::delete_fenced`]), so a
    /// replica that took the pod over once the mark went stale (a slow
    /// API server, a slow look before) never loses it under its RPC. The
    /// caller holds the bring-up lock and has marked the pod.
    async fn leave_and_delete(&self, name: &str, pod: &Pod) -> Result<bool, ControlError> {
        let relay = match self.take_idle_relay(name) {
            Ok(Some(relay)) => Some(relay),
            Ok(None) => match self.attach(name, pod, &Secrets::default()).await {
                Ok(relay) => {
                    drop(relay);
                    self.take_idle_relay(name).ok().flatten()
                }
                Err(e) => {
                    tracing::info!(pod = name, error = %e.message,
                        "cannot reach the engine pod to leave the registry; its record is swept later");
                    None
                }
            },
            Err(()) => {
                self.unmark(name).await;
                return Ok(false);
            }
        };
        let Some(fenced) = self.renew_mark(name).await? else {
            tracing::info!(
                pod = name,
                "the retiring mark is no longer this replica's (another holds the pod); not retiring it"
            );
            return Ok(false);
        };
        if let Some(relay) = relay {
            leave_before_delete(name, relay.client.as_ref()).await;
        }
        self.delete_fenced(name, fenced).await
    }

    /// [`Engines::retire`]: stop pod `name`, which a delete started only
    /// for itself, unless an RPC of any replica uses it.
    pub(super) async fn retire_started(&self, name: &str) -> Result<(), ControlError> {
        let lock = self.bringup_lock(name);
        let _guard = lock.lock().await;
        let Some(pod) = self.mark_retiring(name).await? else {
            tracing::debug!(pod = %name, "engine pod in use; not retiring it");
            return Ok(());
        };
        if self.leave_and_delete(name, &pod).await? {
            tracing::info!(pod = %name, "stopped the engine pod a delete started");
        }
        Ok(())
    }

    /// Whether a PersistentVolume of this driver names filesystem
    /// `fs_uuid` as a pool volume.
    async fn pool_named_by_pv(&self, fs_uuid: &str) -> Result<bool, ControlError> {
        Ok(self
            .records_from_pvs()
            .await?
            .values()
            .any(|r| r.fs_uuid == fs_uuid))
    }

    // ---- pool records ----

    fn configmaps(&self) -> Api<ConfigMap> {
        Api::namespaced(self.client.clone(), &self.cfg.namespace)
    }

    /// The recorded pools, by pod name.
    pub async fn recorded_pools(&self) -> Result<BTreeMap<String, PoolRecord>, ControlError> {
        let cm = self
            .configmaps()
            .get_opt(POOLS_CONFIGMAP)
            .await
            .map_err(|e| kube_err("reading the pool records", e))?;
        Ok(parse_records(
            &cm.and_then(|cm| cm.data).unwrap_or_default(),
        ))
    }

    /// Record (or, `None`, forget) pod `pod`'s pool: one merge patch of
    /// its own key, so replicas recording different pools never conflict.
    async fn write_record(&self, pod: &str, rec: Option<&PoolRecord>) -> Result<(), ControlError> {
        let value = match rec {
            Some(rec) => serde_json::Value::String(
                serde_json::to_string(rec).map_err(|e| ControlError::failed(e.to_string()))?,
            ),
            None => serde_json::Value::Null,
        };
        let patch = serde_json::json!({"data": {pod: value}});
        let api = self.configmaps();
        for _ in 0..3 {
            match api
                .patch(
                    POOLS_CONFIGMAP,
                    &PatchParams::default(),
                    &Patch::Merge(&patch),
                )
                .await
            {
                Ok(_) => break,
                Err(e) if is_status(&e, 404) && rec.is_none() => break,
                Err(e) if is_status(&e, 404) => {
                    let cm = ConfigMap {
                        metadata: ObjectMeta {
                            name: Some(POOLS_CONFIGMAP.into()),
                            ..Default::default()
                        },
                        ..Default::default()
                    };
                    match api.create(&PostParams::default(), &cm).await {
                        Ok(_) => {}
                        Err(e) if is_status(&e, 409) => {}
                        Err(e) => return Err(kube_err("creating the pool records", e)),
                    }
                }
                Err(e) => return Err(kube_err("writing a pool record", e)),
            }
        }
        Ok(())
    }

    /// The record of PV `pv` (`Ok(None)`: not a pool volume of this
    /// driver), its StorageClass read (through `classes`, a per-call cache)
    /// only when the PV itself lacks what the record needs. A pool volume
    /// that yields no record is logged: its pool's trash cannot be found
    /// once its pod is gone.
    async fn record_of_pv(
        &self,
        pv: &PersistentVolume,
        classes: &mut HashMap<String, Option<StorageClass>>,
    ) -> Result<Option<(String, PoolRecord)>, ControlError> {
        if pool_volume_of(pv).is_none() {
            return Ok(None);
        }
        let csi = pv.spec.as_ref().and_then(|s| s.csi.as_ref());
        let has_attributes = csi
            .and_then(|c| c.volume_attributes.as_ref())
            .is_some_and(|a| a.keys().any(|k| !k.contains('/')));
        let has_secret = pv.metadata.annotations.as_ref().is_some_and(|a| {
            a.contains_key(ANNOTATION_DELETION_SECRET_NAME)
                && a.contains_key(ANNOTATION_DELETION_SECRET_NAMESPACE)
        });
        let class_name = pv
            .spec
            .as_ref()
            .and_then(|s| s.storage_class_name.clone())
            .filter(|n| !n.is_empty());
        let mut class = None;
        if !(has_attributes && has_secret) {
            if let Some(class_name) = &class_name {
                if !classes.contains_key(class_name) {
                    let api: Api<StorageClass> = Api::all(self.client.clone());
                    let read = api
                        .get_opt(class_name)
                        .await
                        .map_err(|e| kube_err(&format!("reading StorageClass {class_name}"), e))?;
                    classes.insert(class_name.clone(), read);
                }
                class = classes.get(class_name).cloned().flatten();
            }
        }
        let pv_name = pv.metadata.name.as_deref().unwrap_or("");
        match PoolRecord::from_pv(pv, class.as_ref()) {
            Ok(rec) => {
                if rec.secret.is_none() && !has_secret && class.is_none() {
                    tracing::warn!(pv = pv_name, class = ?class_name,
                        "a pool volume names no deletion secret and its StorageClass is gone: \
                         its pool is recorded without credentials");
                }
                let (_, pod) = rec.pool().map_err(ControlError::failed)?;
                Ok(Some((pod, rec)))
            }
            Err(why) => {
                tracing::warn!(pv = pv_name, class = ?class_name, why,
                    "a pool volume of this driver yields no pool record: its pool's trash \
                     is not purged once its engine pod is gone");
                Ok(None)
            }
        }
    }

    /// The pools this driver's PersistentVolumes name, by pod name: one
    /// record per pool, from the first PV of it that yields one.
    async fn records_from_pvs(&self) -> Result<BTreeMap<String, PoolRecord>, ControlError> {
        let pvs: Api<PersistentVolume> = Api::all(self.client.clone());
        let mut classes = HashMap::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut out = BTreeMap::new();
        let mut params = ListParams::default().limit(500);
        loop {
            let page = pvs
                .list(&params)
                .await
                .map_err(|e| kube_err("listing PersistentVolumes", e))?;
            for pv in &page.items {
                let Some((fs_uuid, _)) = pool_volume_of(pv) else {
                    continue;
                };
                if seen.contains(&fs_uuid) {
                    continue;
                }
                if let Some((pod, rec)) = self.record_of_pv(pv, &mut classes).await? {
                    seen.insert(fs_uuid);
                    out.insert(pod, rec);
                }
            }
            match page.metadata.continue_ {
                Some(token) if !token.is_empty() => params = params.continue_token(&token),
                _ => return Ok(out),
            }
        }
    }

    /// [`Engines::record_trash`]: record the pool of filesystem `fs_uuid`
    /// from the PV of volume `volume_id` (else from any PV naming the
    /// pool). Always written: another replica may have forgotten the
    /// record since (a reap), and one merge patch is idempotent.
    pub(super) async fn record_pool_of(
        &self,
        fs_uuid: &str,
        volume_id: &str,
    ) -> Result<(), ControlError> {
        let mut classes = HashMap::new();
        let mut found = None;
        // The PV of the volume being deleted: still there (external-
        // provisioner removes it only once the delete succeeded), and one
        // read instead of a list of every PV.
        if let Ok(VolumeId::Pool { name, .. }) = VolumeId::parse(volume_id) {
            let pvs: Api<PersistentVolume> = Api::all(self.client.clone());
            let pv = pvs
                .get_opt(&name)
                .await
                .map_err(|e| kube_err("reading the PersistentVolume", e))?;
            if let Some(pv) = pv.filter(|pv| {
                pv.spec
                    .as_ref()
                    .and_then(|s| s.csi.as_ref())
                    .is_some_and(|c| c.volume_handle == volume_id)
            }) {
                found = self.record_of_pv(&pv, &mut classes).await?;
            }
        }
        if found.is_none() {
            found = self
                .records_from_pvs()
                .await?
                .into_iter()
                .find(|(_, r)| r.fs_uuid == fs_uuid);
        }
        let Some((pod, rec)) = found else {
            tracing::warn!(
                fs_uuid,
                volume_id,
                "no PersistentVolume yields a record of this pool; trashing without one"
            );
            return Ok(());
        };
        self.write_record(&pod, Some(&rec)).await
    }

    /// Pod `name` of recorded pool `rec`, brought up when it is not (from
    /// the record), held and attached for the purge worker's calls.
    ///
    /// Credentials only for a pod this replica starts: it unlocks it (from
    /// the credentials this process knows, else the class's provisioner
    /// secret) and follows its Secret's rotations, as the replica whose
    /// request starts a pod does. A pod that runs already is another
    /// replica's or an earlier pass's: it is attached without any, and
    /// unlocked only while it still waits for them. A second unlock or a
    /// second rotation push from this replica would be one more credential
    /// generation for nothing — or, with a Secret read here racing a
    /// rotation, the old key pair put back.
    async fn bring_up_recorded(
        &self,
        name: &str,
        rec: &PoolRecord,
    ) -> Result<PoolClient, ControlError> {
        let (pool, pod) = rec.pool().map_err(ControlError::unavailable)?;
        if pod != name {
            return Err(ControlError::unavailable(format!(
                "the pool recorded under {name} is now {pod}'s (its class changed)"
            )));
        }
        let spec = engine_pod(&pool, &self.cfg, self.owner.as_ref());
        self.specs
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| spec.clone());
        let lock = self.bringup_lock(name);
        let _guard = lock.lock().await;
        let running = self
            .pods
            .get_opt(name)
            .await
            .map_err(|e| kube_err("reading the engine pod", e))?
            .is_some_and(|p| p.metadata.deletion_timestamp.is_none());
        if !running {
            self.watch_rotations(name, &pool);
            let mut creds = self.known_secrets(name).await.is_some();
            if !creds && awaits_unlock(&spec) {
                if let Some((namespace, secret)) = &rec.secret {
                    if let Some(data) = self.read_secret(namespace, secret, &rec.fs_uuid).await {
                        self.remember(name, &data);
                        creds = unlock_params("", &data).is_some();
                    }
                }
            }
            // A pod that would only wait for credentials nobody here has
            // (its class's secret gone or unreadable) is not started: it
            // could neither be purged through nor reaped.
            if awaits_unlock(&spec) && !creds {
                return Err(ControlError::unavailable(format!(
                    "not starting {name}: it needs its credentials and the controller has none \
                     (the class's provisioner secret {:?} is gone or not readable)",
                    rec.secret
                )));
            }
        }
        let pod = self.ensure_ready(name, Some(&spec)).await?;
        let Held::Yes(hold) = self.hold(name, &pod).await? else {
            return Err(retiring_error(name));
        };
        let relay = if running && awaits_unlock(&pod) && self.live_relay(&pod).is_none() {
            self.attach_running(name, &pod).await?
        } else {
            // No secrets of its own: a fresh incarnation is unlocked from
            // the known ones, a running one keeps what it has.
            self.attach(name, &pod, &Secrets::default()).await?
        };
        Ok(PoolClient {
            relay,
            _hold: Some(hold),
        })
    }

    /// Running, ready pod `name` with no record, held and attached for the
    /// purge worker's calls (`None`: not up).
    async fn open_running(&self, name: &str) -> Result<Option<PoolClient>, ControlError> {
        let lock = self.bringup_lock(name);
        let _guard = lock.lock().await;
        let pod = match self.pods.get_opt(name).await {
            Ok(Some(pod)) if pod_ready(&pod) && pod.metadata.deletion_timestamp.is_none() => pod,
            Ok(_) => return Ok(None),
            Err(e) => return Err(kube_err("reading the engine pod", e)),
        };
        let Held::Yes(hold) = self.hold(name, &pod).await? else {
            return Ok(None);
        };
        let relay = if awaits_unlock(&pod) && self.live_relay(&pod).is_none() {
            self.attach_running(name, &pod).await?
        } else {
            self.attach(name, &pod, &Secrets::default()).await?
        };
        Ok(Some(PoolClient {
            relay,
            _hold: Some(hold),
        }))
    }

    /// One pool of a purge pass ([`PurgeBackend::pools`]): recorded pools
    /// brought up, unrecorded running ones attached (`None`: skip it).
    async fn open_pool(
        &self,
        name: &str,
        rec: Option<&PoolRecord>,
        up: bool,
    ) -> Result<Option<PoolClient>, ControlError> {
        let Some(rec) = rec else {
            return self.open_running(name).await;
        };
        // Starting it anew: only while it is still on record (a reap
        // elsewhere, or an uninstall's teardown, may have dropped it since
        // this pass read the records).
        if !up {
            let now = self.recorded_pools().await?;
            if now.get(name) != Some(rec) {
                return Ok(None);
            }
        }
        self.bring_up_recorded(name, rec).await.map(Some)
    }

    /// Recorded pool `name`, which no PV names, failed to come up this
    /// pass: after [`GIVE_UP_PASSES`] such passes in a row spanning
    /// [`GIVE_UP_AFTER`], its record (and its pod, unless held) goes —
    /// its trash, if any, stays in the bucket, logged.
    async fn note_bringup_failure(&self, name: &str, rec: &PoolRecord, why: &str) {
        let give_up = {
            let mut failures = self.bringup_failures.lock().unwrap();
            let (since, passes) = failures
                .entry(name.to_string())
                .or_insert_with(|| (Instant::now(), 0));
            *passes += 1;
            *passes >= GIVE_UP_PASSES && since.elapsed() >= GIVE_UP_AFTER
        };
        if !give_up {
            return;
        }
        // A PV naming it since (a new volume in that pool) keeps it.
        match self.pool_named_by_pv(&rec.fs_uuid).await {
            Ok(false) => {}
            Ok(true) => return,
            Err(e) => {
                tracing::info!(pod = name, error = %e.message, "not dropping a pool record now");
                return;
            }
        }
        let lock = self.bringup_lock(name);
        let guard = lock.lock().await;
        let exists = match self.pods.get_opt(name).await {
            Ok(pod) => pod.is_some(),
            Err(e) => {
                tracing::info!(pod = name, error = %e, "not dropping a pool record now");
                return;
            }
        };
        if exists {
            let marked = match self.mark_retiring(name).await {
                Ok(Some(marked)) => marked,
                Ok(None) => return,
                Err(e) => {
                    tracing::info!(pod = name, error = %e.message, "not dropping a pool record now");
                    return;
                }
            };
            match self.leave_and_delete(name, &marked).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(e) => {
                    tracing::info!(pod = name, error = %e.message, "not dropping a pool record now");
                    return;
                }
            }
        }
        drop(guard);
        match self.write_record(name, None).await {
            Ok(()) => {
                self.bringup_failures.lock().unwrap().remove(name);
                tracing::warn!(pod = name, fs_uuid = %rec.fs_uuid, bucket = ?rec.parameters.get("bucket"),
                    prefix = ?rec.parameters.get("prefix"), why,
                    "dropped the record of a pool no PersistentVolume names and whose engine pod \
                     has not come up for a long time; any trash it still has stays in the bucket")
            }
            Err(e) => tracing::warn!(pod = name, error = %e.message, "dropping a pool record"),
        }
    }

    /// Attach to running pod `name` without unlocking it, unless it still
    /// waits for its credentials ([`Self::bring_up_recorded`]). The caller
    /// holds its bring-up lock.
    async fn attach_running(&self, name: &str, pod: &Pod) -> Result<Arc<Relay>, ControlError> {
        let relay = self.dial_relay(pod).await?;
        match relay.client.fs_list().await {
            Ok(_) => {
                self.learn_uuid(pod, &relay).await?;
                Ok(relay)
            }
            Err(e) if is_awaiting_unlock(&e) => {
                // Waiting indeed: the first unlock of this incarnation.
                self.relays.lock().unwrap().remove(name);
                drop(relay);
                self.attach(name, pod, &Secrets::default()).await
            }
            Err(e) => {
                self.relays.lock().unwrap().remove(name);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl PurgeBackend for EnginePodManager {
    async fn pools(&self, skip: &HashSet<String>) -> Result<Vec<PurgePool>, ControlError> {
        let mut records = self.recorded_pools().await?;
        let mut named_by_pvs: Option<BTreeSet<String>> = None;
        match self.records_from_pvs().await {
            Ok(from_pvs) => {
                named_by_pvs = Some(from_pvs.keys().cloned().collect());
                for (pod, rec) in from_pvs {
                    if records.get(&pod) != Some(&rec) {
                        if let Err(e) = self.write_record(&pod, Some(&rec)).await {
                            tracing::warn!(pod, error = %e.message, "recording a pool");
                        }
                    }
                    records.insert(pod, rec);
                }
            }
            Err(e) => tracing::warn!(error = %e.message,
                "listing the pools PersistentVolumes name; this pass covers the recorded ones"),
        }
        let selector = format!("{LABEL_COMPONENT}=engine,{LABEL_OWNER}=controller");
        let up: BTreeSet<String> = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| kube_err("listing engine pods", e))?
            .items
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .filter_map(|p| p.metadata.name)
            .collect();
        let names: BTreeSet<String> = records.keys().cloned().chain(up.iter().cloned()).collect();
        let candidates: Vec<(String, String)> = names
            .into_iter()
            .filter(|name| !skip.contains(name))
            .filter_map(|name| {
                let unit = name
                    .strip_prefix("constellation-engine-")
                    .and_then(|n| n.strip_suffix("-controller"))?
                    .to_string();
                Some((name, unit))
            })
            .collect();
        // Concurrently: a pool that cannot come up waits out the ready
        // timeout without holding up the others.
        let opened: Vec<_> = futures::stream::iter(candidates)
            .map(|(name, unit)| {
                let rec = records.get(&name);
                let up = up.contains(&name);
                async move {
                    let opened = self.open_pool(&name, rec, up).await;
                    (name, unit, opened)
                }
            })
            .buffer_unordered(BRINGUP_CONCURRENCY)
            .collect()
            .await;
        let mut pools = Vec::new();
        for (name, unit, opened) in opened {
            match opened {
                Ok(Some(client)) => {
                    self.bringup_failures.lock().unwrap().remove(&name);
                    pools.push(PurgePool {
                        pod: name,
                        unit,
                        client: Arc::new(client),
                    })
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::info!(pod = %name, error = %e.message,
                        "not purging through this engine pod now");
                    // Only a record nothing else vouches for is ever given
                    // up on: a PV naming the pool keeps it, and so does a
                    // pass that could not list the PVs.
                    if let (Some(rec), Some(named)) = (records.get(&name), &named_by_pvs) {
                        if !named.contains(&name) {
                            self.note_bringup_failure(&name, rec, &e.message).await;
                        }
                    }
                }
            }
        }
        Ok(pools)
    }

    async fn node_names(&self) -> Result<HashSet<String>, ControlError> {
        use k8s_openapi::api::core::v1::Node;
        let nodes: Api<Node> = Api::all(self.client.clone());
        Ok(nodes
            .list(&ListParams::default())
            .await
            .map_err(|e| kube_err("listing nodes", e))?
            .items
            .into_iter()
            .filter_map(|n| n.metadata.name)
            .collect())
    }

    async fn reap(&self, name: &str) -> Result<bool, ControlError> {
        let lock = self.bringup_lock(name);
        let _guard = lock.lock().await;
        let Some(marked) = self.mark_retiring(name).await? else {
            return Ok(false);
        };
        // Marked: no RPC of any replica can start using it now. Still
        // empty? A PV of it, or anything under /volumes or /.trash, keeps it.
        let still_empty = async {
            if let Some(uuid) = marked
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(LABEL_FS_UUID))
            {
                if self.pool_named_by_pv(uuid).await? {
                    return Ok(false);
                }
            }
            let relay = self.attach(name, &marked, &Secrets::default()).await?;
            for dir in [crate::volume_id::VOLUMES_DIR, crate::volume_id::TRASH_DIR] {
                match relay.client.browse_readdir(dir).await {
                    Ok(listing) if listing.entries.is_empty() => {}
                    Err(e) if crate::purge::not_found(&e) => {}
                    Ok(_) => return Ok(false),
                    Err(e) => return Err(e),
                }
            }
            Ok::<bool, ControlError>(true)
        }
        .await;
        match still_empty {
            Ok(true) => {}
            Ok(false) => {
                self.unmark(name).await;
                return Ok(false);
            }
            Err(e) => {
                self.unmark(name).await;
                return Err(e);
            }
        }
        if !self.leave_and_delete(name, &marked).await? {
            return Ok(false);
        }
        if let Err(e) = self.write_record(name, None).await {
            tracing::warn!(pod = name, error = %e.message,
                "forgetting the reaped pool's record; the next pass brings its pod back and reaps it again");
        }
        Ok(true)
    }

    async fn roll_if_drifted(&self, name: &str) -> Result<bool, ControlError> {
        let lock = self.bringup_lock(name);
        let _guard = lock.lock().await;
        let Some(pod) = self
            .pods
            .get_opt(name)
            .await
            .map_err(|e| kube_err("reading the engine pod", e))?
        else {
            return Ok(false);
        };
        if pod.metadata.deletion_timestamp.is_some() || !drifted_from(&pod, &self.cfg) {
            return Ok(false);
        }
        let Some(marked) = self.mark_retiring(name).await? else {
            return Ok(false);
        };
        let replacement = controller_replacement(&marked, &self.cfg);
        tracing::info!(pod = name, image = %self.cfg.image,
            "replacing a controller-owned engine pod whose settings drifted");
        if !self.leave_and_delete(name, &marked).await? {
            return Ok(false);
        }
        self.wait_deleted(name).await?;
        self.specs
            .lock()
            .unwrap()
            .insert(name.to_string(), replacement.clone());
        let pod = self.ensure_ready(name, Some(&replacement)).await?;
        // Unlocked from what this process holds for it, if anything; else
        // the next request that carries the class's secret does it.
        if let Err(e) = self.attach(name, &pod, &Secrets::default()).await {
            tracing::info!(pod = name, error = %e.message,
                "the replacement waits for a request to unlock it");
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod_with(annotations: &[(&str, String)]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                annotations: Some(
                    annotations
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                ),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn hold_keys_are_valid_annotation_names_per_replica() {
        let a = hold_key("constellation-csi-controller-7d9f-abcde");
        let b = hold_key("constellation-csi-controller-7d9f-fghij");
        assert_ne!(a, b);
        assert_eq!(a, hold_key("constellation-csi-controller-7d9f-abcde"));
        let name = a.split_once('/').unwrap().1;
        assert!(name.len() <= 63);
        assert!(name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'));
    }

    #[test]
    fn only_other_replicas_live_holds_count() {
        let (me, other, third) = (hold_key("me"), hold_key("other"), hold_key("third"));
        let now = 1_000_000_000u64;
        let skew = HOLD_SKEW.as_millis() as u64;
        let pod = pod_with(&[
            (me.as_str(), (now + 60_000).to_string()),
            (other.as_str(), (now + 1).to_string()),
            // Expired, but within the skew allowance: still live.
            (third.as_str(), (now - skew + 1).to_string()),
            ("constellation.dev/engine-config", "x".into()),
        ]);
        let mut live = live_holds(&pod, &me, now);
        live.sort();
        let mut want = vec![other.clone(), third.clone()];
        want.sort();
        assert_eq!(live, want);
        // Past the skew: gone.
        assert_eq!(live_holds(&pod, &me, now + skew + 2), Vec::<String>::new());
        // Unreadable: live (never retire on a value we cannot read).
        let pod = pod_with(&[(other.as_str(), "soon".into())]);
        assert_eq!(live_holds(&pod, &me, now), vec![other]);
    }

    #[test]
    fn a_retiring_mark_goes_stale() {
        let now = 1_000_000_000u64;
        let pod = pod_with(&[(ANNOTATION_RETIRING, format!("{} {now}", hold_key("a")))]);
        let (ttl, skew) = (
            RETIRING_TTL.as_millis() as u64,
            HOLD_SKEW.as_millis() as u64,
        );
        assert_eq!(retiring_by(&pod, now + 1), Some(hold_key("a")));
        // Others honour it for the skew allowance past its TTL; its own
        // replica trusts it only within the TTL by its own clock.
        assert_eq!(retiring_by(&pod, now + ttl), Some(hold_key("a")));
        assert_eq!(retiring_by(&pod, now + ttl + skew), None);
        assert!(own_mark_fresh(&pod, &hold_key("a"), now + ttl - 1));
        assert!(!own_mark_fresh(&pod, &hold_key("a"), now + ttl));
        assert!(!own_mark_fresh(&pod, &hold_key("b"), now + 1));
        assert_eq!(
            retiring_by(&pod_with(&[(ANNOTATION_RETIRING, "junk".into())]), now),
            None
        );
        assert_eq!(retiring_by(&pod_with(&[]), now), None);
        let stripped = without_holds(pod.metadata.annotations.clone()).unwrap();
        assert!(stripped.is_empty());
    }

    /// The protocol's two writes over one object with compare-and-swap
    /// semantics, interleaved every way two replicas can: a hold and a
    /// mark never both succeed while the other is fresh.
    #[test]
    fn a_hold_and_a_mark_never_both_win() {
        // A tiny object store: (resourceVersion, annotations).
        #[derive(Clone)]
        struct Obj {
            rv: u64,
            pod: Pod,
        }
        let (a, b) = (hold_key("replica-a"), hold_key("replica-b"));
        let now = 5_000_000u64;
        // Replica A wants to hold; replica B wants to retire. Each reads,
        // decides, then writes with the version it read.
        for order in 0..4 {
            let mut obj = Obj {
                rv: 1,
                pod: pod_with(&[]),
            };
            let read_a = obj.clone();
            let read_b = obj.clone();
            let hold = |obj: &mut Obj, read: &Obj| -> bool {
                if retiring_by(&read.pod, now).is_some_and(|w| w != a) || read.rv != obj.rv {
                    return false;
                }
                obj.rv += 1;
                obj.pod
                    .metadata
                    .annotations
                    .get_or_insert_with(BTreeMap::new)
                    .insert(a.clone(), (now + 120_000).to_string());
                true
            };
            let mark = |obj: &mut Obj, read: &Obj| -> bool {
                if !live_holds(&read.pod, &b, now).is_empty() || read.rv != obj.rv {
                    return false;
                }
                obj.rv += 1;
                obj.pod
                    .metadata
                    .annotations
                    .get_or_insert_with(BTreeMap::new)
                    .insert(ANNOTATION_RETIRING.into(), format!("{b} {now}"));
                true
            };
            let (hold_won, mark_won) = match order {
                // Both read first, then write in either order.
                0 => {
                    let h = hold(&mut obj, &read_a);
                    (h, mark(&mut obj, &read_b))
                }
                1 => {
                    let m = mark(&mut obj, &read_b);
                    (hold(&mut obj, &read_a), m)
                }
                // One completes before the other reads.
                2 => {
                    let h = hold(&mut obj, &read_a);
                    let read_b = obj.clone();
                    (h, mark(&mut obj, &read_b))
                }
                _ => {
                    let m = mark(&mut obj, &read_b);
                    let read_a = obj.clone();
                    (hold(&mut obj, &read_a), m)
                }
            };
            assert!(
                !(hold_won && mark_won),
                "order {order}: a hold and a retire both went ahead"
            );
            assert!(hold_won || mark_won, "order {order}: somebody wins");
        }
    }

    fn pool_pv(
        fs: &str,
        attributes: &[(&str, &str)],
        annotations: &[(&str, &str)],
    ) -> PersistentVolume {
        use k8s_openapi::api::core::v1::{CSIPersistentVolumeSource, PersistentVolumeSpec};
        let handle = VolumeId::Pool {
            fs_uuid: fs.into(),
            shard: 1,
            name: "pvc-1".into(),
        }
        .to_string();
        let map = |kv: &[(&str, &str)]| -> BTreeMap<String, String> {
            kv.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        PersistentVolume {
            metadata: ObjectMeta {
                name: Some("pvc-1".into()),
                annotations: Some(map(annotations)),
                ..Default::default()
            },
            spec: Some(PersistentVolumeSpec {
                csi: Some(CSIPersistentVolumeSource {
                    driver: crate::identity::DRIVER_NAME.into(),
                    volume_handle: handle,
                    volume_attributes: Some(map(attributes)),
                    ..Default::default()
                }),
                storage_class_name: Some("pool".into()),
                claim_ref: Some(k8s_openapi::api::core::v1::ObjectReference {
                    name: Some("data".into()),
                    namespace: Some("app".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_pool_record_rebuilds_its_pool_and_pod_name() {
        let params: BTreeMap<String, String> = [
            ("bucket", "b"),
            ("prefix", "pool"),
            ("shards", "2"),
            (
                "csi.storage.k8s.io/provisioner-secret-name",
                "creds-${pvc.name}",
            ),
            (
                "csi.storage.k8s.io/provisioner-secret-namespace",
                "${pvc.namespace}",
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let class = StorageClass {
            parameters: Some(params.clone()),
            ..Default::default()
        };
        let pool_params: HashMap<String, String> = params.clone().into_iter().collect();
        let class_params = ClassParams::parse(&pool_params).unwrap();
        let fs = "0190c3b4-0000-7000-8000-000000000001";
        // What a provisioned PV carries: the class parameters minus the
        // CO's keys, and the provisioner's own identity key.
        let attributes = [
            ("bucket", "b"),
            ("prefix", "pool"),
            ("shards", "2"),
            ("storage.kubernetes.io/csiProvisionerIdentity", "1700-csi"),
        ];
        let annotations = [
            (ANNOTATION_DELETION_SECRET_NAME, "creds-data"),
            (ANNOTATION_DELETION_SECRET_NAMESPACE, "app"),
        ];
        let pv = pool_pv(fs, &attributes, &annotations);
        let want = PoolRef {
            class: class_params,
            shard: 1,
            secrets: BTreeMap::new(),
        };

        // The class gone (deleted before its PVCs): nothing is lost.
        let rec = PoolRecord::from_pv(&pv, None).unwrap();
        assert_eq!((rec.fs_uuid.as_str(), rec.shard), (fs, 1));
        assert_eq!(rec.secret, Some(("app".into(), "creds-data".into())));
        assert!(rec.parameters.keys().all(|k| !k.contains('/')));
        let (pool, name) = rec.pool().unwrap();
        assert_eq!(name, pod_name(&want));
        assert_eq!(pool, want);
        // The class still there: the same record (the PV wins).
        assert_eq!(PoolRecord::from_pv(&pv, Some(&class)).unwrap(), rec);

        // No deletion-secret annotations: the class's, resolved for the PV.
        let bare = pool_pv(fs, &attributes, &[]);
        assert_eq!(PoolRecord::from_pv(&bare, Some(&class)).unwrap(), rec);
        assert_eq!(PoolRecord::from_pv(&bare, None).unwrap().secret, None);
        // No attributes either: the class's parameters, else no record.
        let empty = pool_pv(fs, &[], &[]);
        assert_eq!(PoolRecord::from_pv(&empty, Some(&class)).unwrap(), rec);
        assert!(PoolRecord::from_pv(&empty, None).is_err());
        // Attributes that cannot be a pool of that shard: no record.
        assert!(PoolRecord::from_pv(&pool_pv(fs, &[("bucket", "b")], &annotations), None).is_err());

        // Through the ConfigMap and back.
        let data = BTreeMap::from([
            (name.clone(), serde_json::to_string(&rec).unwrap()),
            ("junk".to_string(), "{".to_string()),
        ]);
        assert_eq!(parse_records(&data), BTreeMap::from([(name, rec.clone())]));
        // A class that lost the shard: not rebuildable.
        let mut fewer = rec;
        fewer.parameters.insert("shards".into(), "1".into());
        assert!(fewer.pool().is_err());
    }
}
