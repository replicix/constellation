//! Engine-pod rollouts (plan 37 §8 "Preconditions"): the node plugin
//! replaces each of its engine pods whose spec drifted from the one it
//! would create now — a chart upgrade changed the engine image or one of
//! its settings — by handing its FUSE sessions to a freshly started
//! replacement on the same node ([`super::handoff`]), so a writer sees a
//! stall, never `ENOTCONN`.
//!
//! **The trigger.** The desired spec is the plugin's own engine-pod
//! settings (the chart's `image`/`engineProfile` values, through the
//! DaemonSet's environment); every engine pod carries the fingerprint of
//! the settings it was created from (`constellation.dev/engine-config`).
//! A chart upgrade that changes them rolls the node plugin itself, whose
//! next generation finds every engine pod drifted: [`NodeService::rollout_once`]
//! runs at its start and every `CONSTELLATION_CSI_ROLLOUT_INTERVAL_S`
//! (default 30 s; 0 turns rollouts off).
//!
//! **One pod at a time.** Units are rolled one after another, never two at
//! once on a node, and a unit's stages and unstages wait while its handoff
//! runs (from `Prepare` to the cutover; the replacement's own start —
//! an image pull, say — does not hold them up).
//!
//! **A pod serving no view** (no staged volume, or none it still serves
//! after a restart) is not handed over: it is deleted, and the next stage
//! of its unit — or kubelet's republish — starts one from the desired spec.
//!
//! **Failure (§8 "Failure handling").** A handoff that rolls back leaves
//! the old pod serving and deletes the replacement; the unit is tried
//! again at the next pass, up to [`HandoffConfig::max_attempts`](super::handoff::HandoffConfig::max_attempts) times per
//! desired spec. Then the rollout gives up on that unit: the old pod keeps
//! serving on its old spec — an annotation on it and a Kubernetes event
//! say why, and `constellation_csi_handoff_fallback_total` counts it — and
//! its volumes move to the new spec only the classic way, once the pod
//! ends (idle, crash, an operator's delete) and kubelet's republish
//! restages them onto a new one (`requiresRepublish`, settled decision 12:
//! an `ENOTCONN` window, never worse than before K5). A handoff lost after
//! its commit — the old pod gone, the new one saying it failed, or its pod
//! ended — ends the same way at once: both pods go and the republish
//! restages.
//!
//! **After the commit nothing serving is deleted.** The replacement holds
//! the only copies of the sessions from then on. One that served but could
//! not be adopted yet (its readiness, a dial, the API server — transient),
//! or that neither served nor failed within the resume budget
//! ([`Outcome::Unresolved`]), stays *pending*: every pass asks it again
//! before anything else, adopts it once it serves, and retires the unit
//! only once it says it failed or its pod ended.

use super::handoff::{self, HandoffMetrics, Outcome, ReplacementWatch};
use super::{NodeEngines, NodeService, Replacement};
use constellation_control::proto::types::{
    HandoffParams, HandoffPhase, HandoffState, HandoffTarget,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// What one unit's rollout did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rolled {
    /// Handed over.
    HandedOff(Outcome),
    /// It served no staged volume: deleted, to be started anew on demand.
    Retired,
    /// The replacement did not start (counts as a failed attempt).
    NoReplacement(String),
    /// Given up earlier (attempts exhausted for this desired spec).
    GaveUp,
    /// Cut over earlier, the replacement not adopted yet: still pending.
    Pending,
    /// A pending replacement was adopted.
    Adopted,
}

/// Rollout bookkeeping: attempts per (unit, desired spec), and the
/// replacements committed to but not adopted yet.
#[derive(Default)]
pub struct RolloutState {
    attempts: std::sync::Mutex<HashMap<(String, String), u32>>,
    pending: std::sync::Mutex<HashMap<String, Replacement>>,
    pub metrics: Arc<HandoffMetrics>,
}

/// How many times, and how far apart, a served replacement's adoption is
/// tried within one pass (the rest are the next passes').
const ADOPT_TRIES: u32 = 3;
#[cfg(not(test))]
const ADOPT_PAUSE: Duration = Duration::from_secs(2);
#[cfg(test)]
const ADOPT_PAUSE: Duration = Duration::from_millis(10);

/// [`ReplacementWatch`] over the node's engine pods.
struct PodWatch<'a> {
    engines: &'a dyn NodeEngines,
    unit: &'a str,
    replacement: &'a Replacement,
}

#[async_trait::async_trait]
impl ReplacementWatch for PodWatch<'_> {
    async fn ended(&self) -> Option<String> {
        self.engines
            .replacement_ended(self.unit, self.replacement)
            .await
    }
}

impl RolloutState {
    fn attempts(&self, unit: &str, desired: &str) -> u32 {
        self.attempts
            .lock()
            .unwrap()
            .get(&(unit.to_string(), desired.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// One more failed attempt; the count now.
    fn failed(&self, unit: &str, desired: &str) -> u32 {
        let mut attempts = self.attempts.lock().unwrap();
        let n = attempts
            .entry((unit.to_string(), desired.to_string()))
            .or_default();
        *n += 1;
        *n
    }

    fn done(&self, unit: &str) {
        self.attempts.lock().unwrap().retain(|(u, _), _| u != unit);
    }

    /// Whether `unit` has a replacement committed to and not adopted yet
    /// (the idle GC leaves such a unit alone).
    pub(super) fn is_pending(&self, unit: &str) -> bool {
        self.pending.lock().unwrap().contains_key(unit)
    }
}

/// Whether `unit`'s serving pod serves any view (an unreachable one counts
/// as serving: never retire what cannot be asked).
async fn serves_views(engines: &dyn NodeEngines, unit: &str) -> bool {
    match engines.existing(unit).await {
        Ok(Some(engine)) => match engine.client.view_list(Default::default()).await {
            Ok(listing) => !listing.views.is_empty(),
            // Still waiting for its credentials (37-k6a): it serves
            // nothing yet.
            Err(e) if crate::credentials::is_awaiting_unlock(&e) => false,
            Err(_) => true,
        },
        Ok(None) => false,
        Err(_) => true,
    }
}

/// `CONSTELLATION_CSI_ROLLOUT_INTERVAL_S` (default 30; 0: off).
pub fn interval_from_env() -> Result<Option<Duration>, String> {
    match std::env::var("CONSTELLATION_CSI_ROLLOUT_INTERVAL_S") {
        Err(_) => Ok(Some(Duration::from_secs(30))),
        Ok(v) if v.trim().is_empty() => Ok(Some(Duration::from_secs(30))),
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(0) => Ok(None),
            Ok(s) => Ok(Some(Duration::from_secs(s))),
            Err(_) => Err(format!(
                "CONSTELLATION_CSI_ROLLOUT_INTERVAL_S={v:?} must be seconds"
            )),
        },
    }
}

impl NodeService {
    /// One pass over this node's drifted engine pods (module docs).
    pub async fn rollout_once(&self) -> Vec<(String, Rolled)> {
        let Some(engines) = self.engines.clone() else {
            return Vec::new();
        };
        let drifted = match engines.drifted().await {
            Ok(drifted) => drifted,
            Err(e) => {
                tracing::warn!(error = %e, "listing drifted engine pods");
                return Vec::new();
            }
        };
        let mut done = Vec::new();
        let pending: Vec<(String, Replacement)> = self
            .rollout
            .pending
            .lock()
            .unwrap()
            .iter()
            .map(|(u, r)| (u.clone(), r.clone()))
            .collect();
        // Settled first, and not rolled again in this pass: what `drifted`
        // listed for them predates their settling.
        let mut settled_units = HashSet::new();
        for (unit, replacement) in pending {
            let settled = self.settle(engines.as_ref(), &unit, &replacement).await;
            settled_units.insert(unit.clone());
            done.push((unit, settled));
        }
        for drift in drifted {
            if settled_units.contains(&drift.unit) {
                continue;
            }
            let rolled = self.roll(engines.as_ref(), &drift).await;
            done.push((drift.unit.clone(), rolled));
        }
        done
    }

    /// A replacement committed to and not adopted yet: adopt it once it
    /// serves, retire the unit once it failed or its pod ended, else leave
    /// it pending.
    async fn settle(
        &self,
        engines: &dyn NodeEngines,
        unit: &str,
        replacement: &Replacement,
    ) -> Rolled {
        let status = tokio::time::timeout(
            Duration::from_secs(5),
            replacement.handoff.node_handoff(HandoffParams {
                target: HandoffTarget::Socket,
                phase: Some(HandoffPhase::Status),
                ..HandoffParams::default()
            }),
        )
        .await;
        let lost = match status {
            Ok(Ok(r)) => match r.state {
                Some(HandoffState::Resumed { .. }) => None,
                Some(HandoffState::Failed { reason }) => Some(reason),
                Some(HandoffState::Standby { .. }) => {
                    Some("the replacement restarted: the sessions it held are gone".into())
                }
                _ => return Rolled::Pending,
            },
            _ => match engines.replacement_ended(unit, replacement).await {
                Some(why) => Some(why),
                None => return Rolled::Pending,
            },
        };
        let _gate = self.unit_gate(unit).write_owned().await;
        if let Some(why) = lost {
            tracing::error!(unit, pod = %replacement.pod, why,
                "a pending replacement did not serve; retiring the unit (the republish restages)");
            engines.discard_replacement(unit, replacement.clone()).await;
            if let Err(e) = engines.retire(unit).await {
                tracing::warn!(unit, error = %e, "retiring the unit");
            }
            self.rollout.pending.lock().unwrap().remove(unit);
            self.rollout.done(unit);
            return Rolled::Retired;
        }
        drop(_gate);
        if self.adopt(engines, unit, replacement).await {
            Rolled::Adopted
        } else {
            Rolled::Pending
        }
    }

    /// The replacement serves: make it the unit's pod (§8 step 6, the
    /// plugin's own state repointed), retried a few times; on failure it
    /// stays pending — never retired, it serves.
    async fn adopt(
        &self,
        engines: &dyn NodeEngines,
        unit: &str,
        replacement: &Replacement,
    ) -> bool {
        let mut tries = 0;
        let new = loop {
            tries += 1;
            match engines.adopt_replacement(unit, replacement).await {
                Ok(new) => break new,
                Err(e) if tries < ADOPT_TRIES => {
                    tracing::warn!(unit, pod = %replacement.pod, error = %e, tries,
                        "adopting the replacement; retrying");
                    tokio::time::sleep(ADOPT_PAUSE).await;
                }
                Err(e) => {
                    tracing::error!(unit, pod = %replacement.pod, error = %e,
                        "the replacement serves but could not be adopted yet; the next rollout \
                         pass tries again");
                    self.rollout
                        .pending
                        .lock()
                        .unwrap()
                        .insert(unit.to_string(), replacement.clone());
                    return false;
                }
            }
        };
        {
            let _gate = self.unit_gate(unit).write_owned().await;
            for mut record in self.state.all().into_iter().filter(|r| r.unit == unit) {
                record.pod = new.pod.clone();
                if let Err(e) = self.state.put(record) {
                    tracing::warn!(error = %e, "repointing a staged-volume record");
                }
            }
        }
        self.rollout.pending.lock().unwrap().remove(unit);
        self.record_views(engines, unit, &new.pod).await;
        self.rollout.done(unit);
        true
    }

    async fn roll(&self, engines: &dyn NodeEngines, drift: &super::engines::Drift) -> Rolled {
        let cfg = self.handoff;
        let unit = &drift.unit;
        if self.rollout.attempts(unit, &drift.desired) >= cfg.max_attempts {
            return Rolled::GaveUp;
        }
        let staged = self.state.all().iter().filter(|r| &r.unit == unit).count();
        if staged == 0 || !serves_views(engines, unit).await {
            let _gate = self.unit_gate(unit).write_owned().await;
            // Asked again under the gate: a stage may have just landed.
            if serves_views(engines, unit).await {
                return Rolled::NoReplacement("a volume was staged meanwhile".into());
            }
            // Nothing to hand over: no staged volume, or none the pod still
            // serves (it restarted, and their mounts are dead already —
            // kubelet's republish restages them onto the next pod).
            tracing::info!(unit, pod = %drift.pod, why = %drift.why, staged,
                "engine pod drifted and serves no view: deleting it");
            return match engines.retire(unit).await {
                Ok(()) => Rolled::Retired,
                Err(e) => Rolled::NoReplacement(e.to_string()),
            };
        }
        tracing::info!(unit, pod = %drift.pod, why = %drift.why, staged,
            "engine pod drifted: handing its sessions to a replacement");
        let replacement = match engines.start_replacement(unit).await {
            Ok(r) => r,
            Err(e) => {
                let why = format!("starting the replacement: {e}");
                self.attempt_failed(engines, drift, &why).await;
                return Rolled::NoReplacement(why);
            }
        };
        let gate = self.unit_gate(unit).write_owned().await;
        let old = match engines.existing(unit).await {
            Ok(Some(old)) => old,
            Ok(None) | Err(_) => {
                // Gone meanwhile (a crash): its republish restages onto
                // whatever runs next; the replacement is not needed.
                engines.discard_replacement(unit, replacement).await;
                return Rolled::NoReplacement("the serving pod went away".into());
            }
        };
        let watch = PodWatch {
            engines,
            unit,
            replacement: &replacement,
        };
        let outcome = handoff::run(
            old.client.as_ref(),
            replacement.handoff.as_ref(),
            &watch,
            &cfg,
        )
        .await;
        self.rollout.metrics.record(&outcome);
        match &outcome {
            Outcome::Succeeded { views, elapsed } => tracing::info!(unit, views, ?elapsed,
                from = %old.pod, to = %replacement.pod, "engine-pod handoff succeeded"),
            Outcome::Partial {
                views,
                failed,
                elapsed,
            } => tracing::error!(
                unit,
                views,
                ?failed,
                ?elapsed,
                "engine-pod handoff: some views were not resumed; kubelet's republish \
                restages them"
            ),
            Outcome::RolledBack {
                step,
                error,
                restored,
            } => tracing::warn!(
                unit,
                step = step.name(),
                error,
                restored,
                "engine-pod handoff rolled back; the old \
                pod serves"
            ),
            Outcome::Lost { step, error } => tracing::error!(
                unit,
                step = step.name(),
                error,
                "engine-pod handoff lost after its commit; kubelet's republish restages the \
                volumes"
            ),
            Outcome::Unresolved { error } => tracing::error!(
                unit,
                pod = %replacement.pod,
                error,
                "engine-pod handoff committed but not resumed yet; the replacement is left \
                pending (it holds the sessions)"
            ),
        }
        drop(gate);
        match &outcome {
            _ if outcome.cut_over() => {
                self.adopt(engines, unit, &replacement).await;
            }
            Outcome::Unresolved { .. } => {
                self.rollout
                    .pending
                    .lock()
                    .unwrap()
                    .insert(unit.to_string(), replacement);
            }
            Outcome::Lost { .. } => {
                let _gate = self.unit_gate(unit).write_owned().await;
                engines.discard_replacement(unit, replacement).await;
                if let Err(e) = engines.retire(unit).await {
                    tracing::warn!(unit, error = %e, "deleting the handed-off pod");
                }
                self.rollout.done(unit);
            }
            _ => {
                engines.discard_replacement(unit, replacement).await;
                if let Outcome::RolledBack { error, .. } = &outcome {
                    self.attempt_failed(engines, drift, error).await;
                }
            }
        }
        Rolled::HandedOff(outcome)
    }

    async fn attempt_failed(
        &self,
        engines: &dyn NodeEngines,
        drift: &super::engines::Drift,
        why: &str,
    ) {
        let n = self.rollout.failed(&drift.unit, &drift.desired);
        if n >= self.handoff.max_attempts {
            let message = format!(
                "giving up on handing {} over after {n} attempts (last: {why}); it keeps serving \
                 its volumes on the old spec until it ends, and kubelet's republish then restages \
                 them on a new pod",
                drift.pod
            );
            tracing::error!(unit = %drift.unit, "{message}");
            self.rollout
                .metrics
                .fallback
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            engines.report_fallback(&drift.unit, &message).await;
        }
    }

    /// Run [`Self::rollout_once`] now and every `interval`.
    pub fn spawn_rollout(service: Arc<NodeService>, interval: Duration) {
        tokio::spawn(async move {
            loop {
                let rolled = service.rollout_once().await;
                // A unit given up on was said so once, when it was
                // (`attempt_failed`); not every pass after.
                let acted: Vec<_> = rolled
                    .iter()
                    .filter(|(_, r)| !matches!(r, Rolled::GaveUp | Rolled::Pending))
                    .collect();
                if !acted.is_empty() {
                    tracing::info!(rolled = ?acted, "engine-pod rollout pass");
                }
                tokio::time::sleep(interval).await;
            }
        });
    }
}
