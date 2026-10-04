//! Engine-pod idle GC and node drain (plan 37 §7 "Ownership and GC",
//! "Drain"; 37-k6b): the node plugin ends the engine pods of its node that
//! no longer serve anything, and ends them by **leaving the pool's
//! registry** first.
//!
//! **One pass** ([`NodeService::gc_once`], every
//! `CONSTELLATION_CSI_IDLE_GC_INTERVAL_S`, default 30 s) looks at every
//! engine pod of this node ([`NodeEngines::units`], found by its labels, so
//! a restarted plugin owns what its predecessor made). A unit is collected
//! when
//!
//! - no volume this plugin staged is recorded against it (its
//!   [`super::state::StateStore`]): a staged volume means a mounted view;
//! - no rollout of it is pending (its replacement holds the sessions);
//! - and either the node is **draining** ([`NodeEngines::draining`]:
//!   cordoned, tainted for deletion by the autoscaler, out of service, or
//!   being deleted), or its `last-view-count` annotation has said 0 for
//!   longer than `--engine-pod-idle-ttl` (`CONSTELLATION_CSI_ENGINE_IDLE_TTL`,
//!   the chart's `engineProfile.idleTtl`, default 10 minutes; a pod never
//!   counted is idle since it was created).
//!
//! **The annotation is never trusted alone** (the 37-k3b review): under the
//! unit's gate, held exclusively so no stage or unstage of it runs
//! meanwhile, the pod is asked `view.list`, and a pod that serves any view
//! is kept. A pod that cannot be asked (not ready, unreachable) is kept too
//! — never retire what cannot be asked — except that one which is not
//! ready and has no volume recorded is deleted (a crash-looping or
//! never-started pod), keeping its node identity for its next pod.
//!
//! **Collecting is `node.leave`, then the pod goes.** A pod deleted without
//! leaving would stay in the pool's roster as a write-eligible member that
//! never comes back — blocking continuation epochs while it is "away" and
//! a ghost forever once its node is gone (the design brief's "autoscaled
//! churn must not bloat the roster"). So the plugin first moves the unit's
//! node identity aside ([`NodeEngines::set_identity_aside`]: from then on
//! no new pod of the unit can start on it), asks the engine for
//! `node.leave{force: false}` (it flushes its journal, releases its leases
//! and tombstones its registry record), then [`NodeEngines::forget_unit`]
//! deletes the pod and removes the identity: the unit's next pod is a
//! fresh node that replays the pool's log. A refused leave (an open
//! continuation epoch, a stranded journal) puts the identity back and
//! keeps the pod, and the next pass tries again.
//!
//! **A pass that ends between those steps** (the plugin stopped or
//! crashed) leaves the identity aside. Each pass first settles such units:
//! a pod still up is asked `node.status` — left already: forgotten;
//! enrolled: the identity goes back. A fresh identity in place next to it
//! (a stage started a pod on a new one first) means the aside one is an
//! older incarnation's that can never go back: it is retired, the fresh
//! one and its pod untouched (a ghost record of the old one is logged as
//! possible). With no pod to ask (or none
//! answering for five minutes) the unit is forgotten: a new pod then
//! starts as a fresh node, which at worst leaves the old record behind if
//! the leave never ran — never a pod crash-looping on a state dir that
//! has left. An engine still waiting
//! for its credentials has not joined anything: it is deleted, and its
//! identity — which a previous incarnation may have enrolled — is kept.
//! Identity survives everything else — a crash, an OOM, a rollout's
//! handoff — which is what makes the handoff a same-node affair (§7 "Node
//! identity").
//!
//! **Drain.** `kubectl drain` evicts the workload pods (kubelet then
//! unstages their volumes through this plugin) but not the engine pods: the
//! chart's `PodDisruptionBudget` refuses every eviction of a node-owned
//! engine pod, so the drain waits — retrying, as it does for any budget —
//! until this plugin, seeing the node cordoned, has collected each engine
//! pod whose views are gone (the eviction order §7 asks for: workloads
//! first, then the engines, then the node). The plugin's own `preStop`
//! (`constellation-csi --pre-stop`) waits for the same on a node that is
//! going for good ([`Drain::Condemned`]: being deleted, tainted for
//! deletion or out of service), and returns at once otherwise — a cordon
//! alone included: the drain leaves the plugin (a DaemonSet pod) alone, so
//! the plugin stopping on a cordoned node is a rollout of it, which must
//! not wait for volumes that stay staged. Engine pods are bare pods, so `kubectl drain`
//! needs `--force` to touch them at all; without it the drain refuses to
//! start rather than evicting anything.

use super::{Drain, NodeEngines, NodeService, UnitPod};
use constellation_control::proto::types::{LeaveParams, ViewListParams};
use std::sync::Arc;
use std::time::Duration;

/// What one pass did with one unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Collected {
    /// Left the registry; pod deleted and identity retired.
    Left,
    /// Deleted without leaving (it waited for its credentials, or was not
    /// ready), identity kept.
    Deleted,
    /// Kept: why.
    Kept(String),
}

/// The idle GC's settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcConfig {
    /// Between passes.
    pub interval: Duration,
    /// How long a pod must have served nothing; `None`: idle pods are never
    /// collected (a drain still is).
    pub idle_ttl: Option<Duration>,
}

impl Default for GcConfig {
    fn default() -> Self {
        GcConfig {
            interval: Duration::from_secs(30),
            idle_ttl: Some(Duration::from_secs(600)),
        }
    }
}

impl GcConfig {
    /// `CONSTELLATION_CSI_ENGINE_IDLE_TTL` (a duration; `0`: idle pods
    /// stay) and `CONSTELLATION_CSI_IDLE_GC_INTERVAL_S` (seconds).
    pub fn from_env() -> Result<GcConfig, String> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<GcConfig, String> {
        let var = |k: &str| var(k).filter(|v| !v.trim().is_empty());
        let mut cfg = GcConfig::default();
        if let Some(v) = var("CONSTELLATION_CSI_ENGINE_IDLE_TTL") {
            let ttl = crate::params::parse_duration(&v)
                .map_err(|e| format!("CONSTELLATION_CSI_ENGINE_IDLE_TTL: {e}"))?;
            cfg.idle_ttl = (!ttl.is_zero()).then_some(ttl);
        }
        if let Some(v) = var("CONSTELLATION_CSI_IDLE_GC_INTERVAL_S") {
            let secs: u64 = v.trim().parse().map_err(|_| {
                format!("CONSTELLATION_CSI_IDLE_GC_INTERVAL_S={v:?} must be seconds")
            })?;
            cfg.interval = Duration::from_secs(secs.max(1));
        }
        Ok(cfg)
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Since when `pod` has served nothing by its annotations, `None` while it
/// serves something.
fn idle_since(pod: &UnitPod) -> Option<u64> {
    match pod.views {
        Some(0) => pod.idle_since.or(pod.created),
        Some(_) => None,
        None => pod.created,
    }
}

impl NodeService {
    /// Whether a volume this plugin staged is recorded against `unit`.
    fn unit_has_volumes(&self, unit: &str) -> bool {
        self.state.all().iter().any(|r| r.unit == unit)
    }

    /// One pass (module docs) at `now` (unix seconds).
    pub async fn gc_once(&self, cfg: &GcConfig, now: u64) -> Vec<(String, Collected)> {
        let Some(engines) = self.engines.clone() else {
            return Vec::new();
        };
        let draining = match engines.draining().await {
            Ok(d) => d != Drain::No,
            Err(e) => {
                tracing::warn!(error = %e, "reading this node's state; treating it as not draining");
                false
            }
        };
        let units = match engines.units().await {
            Ok(units) => units,
            Err(e) => {
                tracing::warn!(error = %e, "listing this node's engine pods");
                return Vec::new();
            }
        };
        let mut done = self.settle_aside(engines.as_ref(), &units, now).await;
        let settled: Vec<String> = done.iter().map(|(u, _)| u.clone()).collect();
        for pod in units.iter().filter(|p| !settled.contains(&p.unit)) {
            let outcome = self
                .collect(engines.as_ref(), pod, cfg, now, draining)
                .await;
            if let Some(outcome) = outcome {
                done.push((pod.unit.clone(), outcome));
            }
        }
        done
    }

    /// Units whose identity a pass left aside (module docs): back, or
    /// forgotten.
    async fn settle_aside(
        &self,
        engines: &dyn NodeEngines,
        units: &[UnitPod],
        now: u64,
    ) -> Vec<(String, Collected)> {
        let aside = match engines.identities_aside().await {
            Ok(aside) => aside,
            Err(e) => {
                tracing::warn!(error = %e, "listing the node identities set aside");
                return Vec::new();
            }
        };
        let mut done = Vec::new();
        for (unit, since) in aside {
            let Ok(_gate) = self.unit_gate(&unit).try_write_owned() else {
                continue;
            };
            // A fresh identity next to it: the engine asked below would be
            // the new incarnation, and the aside one can never go back.
            match engines.discard_superseded_aside(&unit).await {
                Ok(false) => {}
                Ok(true) => {
                    done.push((
                        unit,
                        Collected::Kept("a fresh identity superseded the one aside".into()),
                    ));
                    continue;
                }
                Err(e) => {
                    done.push((
                        unit,
                        Collected::Kept(format!("retiring the superseded identity: {e}")),
                    ));
                    continue;
                }
            }
            let has_pod = units.iter().any(|u| u.unit == unit);
            let enrolled = match engines.existing(&unit).await {
                Ok(Some(engine)) => engine.client.node_enrolled().await.ok(),
                _ => None,
            };
            let outcome = match enrolled {
                Some(true) => match engines.restore_identity(&unit).await {
                    Ok(()) => {
                        tracing::info!(unit, "the engine never left; its node identity is back");
                        Collected::Kept("its leave never ran".into())
                    }
                    Err(e) => Collected::Kept(format!("restoring its identity: {e}")),
                },
                // Left already, or nothing (for long enough) to ask.
                Some(false) => self.forget(engines, &unit, "it left already").await,
                None if !has_pod || now >= since.saturating_add(300) => {
                    self.forget(engines, &unit, "no engine to ask").await
                }
                None => continue,
            };
            done.push((unit, outcome));
        }
        done
    }

    /// [`NodeEngines::forget_unit`] of a unit that left (or may have).
    async fn forget(&self, engines: &dyn NodeEngines, unit: &str, why: &str) -> Collected {
        tracing::info!(
            unit,
            why,
            "retiring a node identity left aside by an earlier pass"
        );
        match engines.forget_unit(unit).await {
            Ok(()) => Collected::Left,
            Err(e) => Collected::Kept(format!("retiring its identity: {e}")),
        }
    }

    /// `pod`'s unit, collected if it is due (`None`: not even a candidate).
    async fn collect(
        &self,
        engines: &dyn NodeEngines,
        pod: &UnitPod,
        cfg: &GcConfig,
        now: u64,
        draining: bool,
    ) -> Option<Collected> {
        let unit = pod.unit.as_str();
        if self.unit_has_volumes(unit) || self.rollout.is_pending(unit) {
            return None;
        }
        if !draining {
            let ttl = cfg.idle_ttl?.as_secs();
            let since = idle_since(pod)?;
            if now < since.saturating_add(ttl) {
                return None;
            }
        }
        // Exclusive: no stage or unstage of the unit, no handoff, while it
        // is asked and collected. Busy now: the next pass.
        let Ok(_gate) = self.unit_gate(unit).try_write_owned() else {
            return Some(Collected::Kept("the unit is busy".into()));
        };
        if self.unit_has_volumes(unit) {
            return Some(Collected::Kept("a volume was staged meanwhile".into()));
        }
        let why = if draining {
            "the node is draining"
        } else {
            "idle"
        };
        let engine = match engines.existing(unit).await {
            Ok(Some(engine)) => engine,
            Ok(None) if !pod.ready => {
                tracing::info!(unit, pod = %pod.pod, why,
                    "deleting an engine pod that is not ready and serves no staged volume");
                return Some(match engines.retire(unit).await {
                    Ok(()) => Collected::Deleted,
                    Err(e) => Collected::Kept(format!("deleting it: {e}")),
                });
            }
            Ok(None) => return Some(Collected::Kept("no serving pod".into())),
            Err(e) => return Some(Collected::Kept(format!("cannot reach it: {e}"))),
        };
        match engine.client.view_list(ViewListParams::default()).await {
            Ok(listing) if !listing.views.is_empty() => {
                tracing::info!(unit, pod = %engine.pod, views = listing.views.len(),
                    "engine pod looked idle but serves views; kept");
                return Some(Collected::Kept(format!(
                    "serves {} view(s)",
                    listing.views.len()
                )));
            }
            Ok(_) => {}
            Err(e) if crate::credentials::is_awaiting_unlock(&e) => {
                tracing::info!(unit, pod = %engine.pod, why,
                    "deleting an engine pod still waiting for its credentials");
                return Some(match engines.retire(unit).await {
                    Ok(()) => Collected::Deleted,
                    Err(e) => Collected::Kept(format!("deleting it: {e}")),
                });
            }
            Err(e) => return Some(Collected::Kept(format!("view.list: {e}"))),
        }
        // The identity aside before the leave (module docs): a pass that
        // ends anywhere after this is settled by the next one.
        if let Err(e) = engines.set_identity_aside(unit).await {
            return Some(Collected::Kept(format!("moving its identity aside: {e}")));
        }
        if let Err(e) = engine
            .client
            .node_leave(LeaveParams {
                node_id: None,
                force: false,
            })
            .await
        {
            tracing::warn!(unit, pod = %engine.pod, why, error = %e,
                "the engine pod could not leave the registry; kept for the next pass");
            if let Err(e) = engines.restore_identity(unit).await {
                tracing::warn!(unit, error = %e, "restoring the node identity after a refused leave");
            }
            return Some(Collected::Kept(format!("node.leave: {e}")));
        }
        tracing::info!(unit, pod = %engine.pod, why,
            "engine pod left the pool's registry; deleting it");
        match engines.forget_unit(unit).await {
            Ok(()) => Some(Collected::Left),
            Err(e) => {
                // It left: the identity was moved aside before the leave,
                // so nothing starts on it again; the next pass finishes
                // (the engine reports it left).
                tracing::warn!(unit, error = %e, "retiring the unit after its leave");
                Some(Collected::Left)
            }
        }
    }

    /// Run [`Self::gc_once`] every `cfg.interval`.
    pub fn spawn_gc(service: Arc<NodeService>, cfg: GcConfig) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cfg.interval).await;
                let done = service.gc_once(&cfg, unix_now()).await;
                let acted: Vec<_> = done
                    .iter()
                    .filter(|(_, c)| !matches!(c, Collected::Kept(_)))
                    .collect();
                if !acted.is_empty() {
                    tracing::info!(collected = ?acted, "engine-pod GC pass");
                }
            }
        });
    }
}

/// `constellation-csi --pre-stop` (the node plugin's `preStop` hook): on a
/// node going for good ([`Drain::Condemned`]), wait until it has no engine
/// pod left (the running plugin collects them, module docs) or `timeout`
/// passes; on any other node — a merely cordoned one too — return at once
/// (a plugin restart must not wait for anything).
pub async fn pre_stop(engines: &dyn NodeEngines, timeout: Duration) -> Result<(), String> {
    let drain = engines.draining().await.map_err(|e| e.to_string())?;
    if drain != Drain::Condemned {
        tracing::info!(
            ?drain,
            "preStop: the node is not going away; nothing to wait for"
        );
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let left = engines.units().await.map_err(|e| e.to_string())?;
        if left.is_empty() {
            tracing::info!("preStop: no engine pod left on the draining node");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let names: Vec<_> = left.iter().map(|u| u.pod.as_str()).collect();
            return Err(format!(
                "preStop: engine pods still on the draining node after {timeout:?}: {names:?}"
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
