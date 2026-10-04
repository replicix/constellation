//! [`NodeEngines`]: how the node plugin reaches the engine pods it owns —
//! one per (pool filesystem [shard], node), `constellation-engine-<unit>-<node>`
//! (plan 37 §4, §7) — and [`InMemoryNodeEngines`], the fake the unit tests
//! and `--in-memory-backend` run on. The real one is
//! [`crate::engine_pods::NodeEnginePods`].
//!
//! Unlike the controller's [`crate::control_client::Engines`], every client
//! here must be able to pass a descriptor: `NodeStageVolume` sends the
//! `/dev/fuse` descriptor of the staging mount with `view.mount`, which only
//! the engine pod's hostPath unix socket can carry.

use crate::control_client::{ControlClient, InMemoryControl, PoolRef};
use crate::credentials::{fingerprint, unlock_params, Secrets};
use crate::engine_pods::{unit_name, unlock_target};
use async_trait::async_trait;
use constellation_control::proto::ControlError;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A connection to one engine pod incarnation.
#[derive(Clone)]
pub struct NodeEngine {
    pub client: Arc<dyn ControlClient>,
    /// The unit (`<hostRoot>/sockets/<unit>/`) and the pod's name.
    pub unit: String,
    pub pod: String,
    /// Which incarnation of the pod this is (its uid and its engine
    /// container's id: a container restart counts). A new one has lost
    /// every view and every credential `fs.unlock` gave the old one.
    pub incarnation: String,
}

#[async_trait]
pub trait NodeEngines: Send + Sync {
    /// The unit and pod name serving `pool` on this node.
    fn names(&self, pool: &PoolRef) -> (String, String);

    /// The engine pod serving `pool` on this node, created first when there
    /// is none (§7 "Creation"), once it is ready. `fs_uuid` is the
    /// filesystem the volume being staged names (the pod's
    /// `constellation.dev/fs-uuid` label; the caller still checks that the
    /// pod serves it). `unlock`: the credentials to send a pod that waits
    /// for them (`crate::credentials`) — sent once per incarnation, and
    /// again when they differ from what it got (a rotation). Without them
    /// a waiting pod is returned as it is, and answers
    /// [`constellation_control::proto::AWAITING_UNLOCK`].
    async fn engine(
        &self,
        pool: &PoolRef,
        fs_uuid: &str,
        unlock: Option<&Secrets>,
    ) -> Result<NodeEngine, ControlError>;

    /// The engine pod of `unit` when it is running and ready; `None` when
    /// it is not (gone, or not ready yet). Never creates one.
    async fn existing(&self, unit: &str) -> Result<Option<NodeEngine>, ControlError>;

    /// Push `secrets` to the running pod of `unit` (a `refreshing` class's
    /// rotation): `false` when there is no such pod, or it needs no
    /// credentials. Never creates one.
    async fn rotate(&self, unit: &str, secrets: &Secrets) -> Result<bool, ControlError>;

    /// Record how many staged volumes the pod of `unit` serves (§7's
    /// `last-view-count` annotation, and since when it has served none: the
    /// idle GC of 37-k6b reads them).
    async fn set_view_count(&self, unit: &str, views: usize) -> Result<(), ControlError>;

    // ---- plan 37 §7: idle GC and drain (crate::node::gc, 37-k6b) ----

    /// This node's engine pods, one per unit (its newest pod), with what
    /// the idle GC reads off them. Found by their labels, so a restarted
    /// plugin owns the pods its predecessor made.
    async fn units(&self) -> Result<Vec<UnitPod>, ControlError> {
        Ok(Vec::new())
    }

    /// Whether this node is being drained ([`Drain`]).
    async fn draining(&self) -> Result<Drain, ControlError> {
        Ok(Drain::No)
    }

    /// Before `unit`'s engine is asked to leave the pool's registry: move
    /// its node identity (`<hostRoot>/node-identity/<unit>/`) aside to
    /// `.leaving-<unit>`, atomically, so that from the leave on no new pod
    /// can start on a state dir that may have left. The running pod keeps
    /// using it (its mount follows the directory, not the name).
    async fn set_identity_aside(&self, unit: &str) -> Result<(), ControlError> {
        let _ = unit;
        Ok(())
    }

    /// The leave was refused: `unit`'s identity goes back where it was
    /// (an error when a fresh identity took its place meanwhile).
    async fn restore_identity(&self, unit: &str) -> Result<(), ControlError> {
        let _ = unit;
        Ok(())
    }

    /// `unit`'s identity aside belongs to an older incarnation when a fresh
    /// identity is in place next to it (a pod started on a new one while
    /// it was aside): it can never be restored, so retire it — without
    /// touching the fresh identity or its pods. `false`: no fresh identity,
    /// nothing done.
    async fn discard_superseded_aside(&self, unit: &str) -> Result<bool, ControlError> {
        let _ = unit;
        Ok(false)
    }

    /// Units whose identity is aside for a leave (a pass that ended between
    /// moving it and finishing), with since when (unix seconds).
    async fn identities_aside(&self) -> Result<Vec<(String, u64)>, ControlError> {
        Ok(Vec::new())
    }

    /// `unit`'s engine has left the pool's registry: retire its node
    /// identity (whether still in place or aside), delete every pod of the
    /// unit, and remove the old identity once they are gone. The unit's
    /// next pod starts as a fresh node.
    async fn forget_unit(&self, unit: &str) -> Result<(), ControlError> {
        let _ = unit;
        Ok(())
    }

    // ---- plan 37 §8: rollouts (crate::node::rollout) ----

    /// This node's engine pods (ready, serving) whose spec differs from the
    /// one this plugin would create now — an image or engine setting
    /// changed by a chart upgrade.
    async fn drifted(&self) -> Result<Vec<Drift>, ControlError> {
        Ok(Vec::new())
    }

    /// Start `unit`'s replacement from the desired spec, beside the pod
    /// serving it; once it waits as a handoff standby (the state dir is
    /// the serving pod's), its handoff socket's client.
    async fn start_replacement(&self, unit: &str) -> Result<Replacement, ControlError> {
        Err(ControlError::unsupported(format!(
            "no rollouts here ({unit})"
        )))
    }

    /// The replacement has taken over (the handoff cut over): once it is
    /// ready, it becomes `unit`'s pod and the old one is deleted. Its
    /// connection.
    async fn adopt_replacement(
        &self,
        unit: &str,
        replacement: &Replacement,
    ) -> Result<NodeEngine, ControlError> {
        let _ = replacement;
        Err(ControlError::unsupported(format!(
            "no rollouts here ({unit})"
        )))
    }

    /// The replacement is not needed (a rolled-back handoff): delete it.
    async fn discard_replacement(&self, unit: &str, replacement: Replacement) {
        let _ = (unit, replacement);
    }

    /// Whether the replacement's pod has ended — deleted, terminated, or
    /// its container restarted (what it received is gone then): `Some(why)`
    /// once it has.
    async fn replacement_ended(&self, unit: &str, replacement: &Replacement) -> Option<String> {
        let _ = (unit, replacement);
        None
    }

    /// Delete `unit`'s serving pod outright: it serves no staged volume
    /// (the next stage starts one from the desired spec), or a handoff was
    /// lost after its commit.
    async fn retire(&self, unit: &str) -> Result<(), ControlError> {
        Err(ControlError::unsupported(format!(
            "no rollouts here ({unit})"
        )))
    }

    /// A rollout gave up on `unit` (§8 "Failure handling"): say so where an
    /// operator looks (the pod's annotations, an event).
    async fn report_fallback(&self, unit: &str, why: &str) {
        let _ = (unit, why);
    }
}

/// How a node is going away (plan 37 §7 "Drain").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drain {
    No,
    /// Cordoned (`unschedulable`): `kubectl drain` evicts its workloads,
    /// and the idle GC collects each engine pod once its views are gone —
    /// but the node plugin (a DaemonSet pod the drain leaves alone) stopping
    /// now is a plain restart (a rollout of the plugin on a cordoned node).
    Cordoned,
    /// Going for good: tainted for deletion by the cluster autoscaler, out
    /// of service, or being deleted. The plugin's `preStop` waits for its
    /// engine pods to be collected.
    Condemned,
}

/// One unit's engine pod on this node, as the idle GC sees it
/// ([`NodeEngines::units`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitPod {
    pub unit: String,
    pub pod: String,
    /// Running and ready (a pod that is not cannot be asked anything).
    pub ready: bool,
    /// `constellation.dev/last-view-count`; `None`: never counted (a stage
    /// that failed after bringing it up).
    pub views: Option<u64>,
    /// `constellation.dev/idle-since` (unix seconds), set with a count of 0.
    pub idle_since: Option<u64>,
    /// When the pod was created (unix seconds).
    pub created: Option<u64>,
}

/// An engine pod whose spec drifted ([`NodeEngines::drifted`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drift {
    pub unit: String,
    pub pod: String,
    /// The desired spec's fingerprint: what a rollout's attempts count
    /// against (a further change starts the count again).
    pub desired: String,
    /// What differs, for the log.
    pub why: String,
}

/// A replacement engine pod, waiting as a handoff standby.
#[derive(Clone)]
pub struct Replacement {
    pub pod: String,
    /// Its handoff socket (`node.handoff`'s receiving phases).
    pub handoff: Arc<dyn ControlClient>,
}

struct FakeEngine {
    incarnation: u64,
    control: Arc<InMemoryControl>,
    /// The spec it was started with (a test changes the desired one with
    /// [`InMemoryNodeEngines::set_desired`]).
    spec: String,
    /// Its replacement, waiting as a standby.
    standby: Option<Arc<InMemoryControl>>,
    /// Started as `--await-unlock` (its class needs credentials), so every
    /// new incarnation waits for an `fs.unlock` again.
    awaits: bool,
    /// The `--s3` URL its `fs.unlock` names.
    target: String,
    /// Up (false once [`NodeEngines::forget_unit`] removed it; the tree,
    /// the pool's S3 data, stays for the next one).
    running: bool,
    /// Started at (unix seconds).
    started_at: u64,
    /// (incarnation, fingerprint) of the last unlock.
    unlocked: Option<(u64, [u8; 32])>,
}

/// [`NodeEngines`] over [`InMemoryControl`]s, one per unit, serving the
/// filesystem the first volume named (module docs). [`Self::crash`] plays a
/// pod dying: the next [`NodeEngines::engine`] is a new incarnation with no
/// view.
pub struct InMemoryNodeEngines {
    node: String,
    /// `--in-memory-backend`: the volumes live in another process (the
    /// controller's fake), so every path is taken to exist.
    any_path: bool,
    engines: Mutex<BTreeMap<String, FakeEngine>>,
    views: Mutex<BTreeMap<String, usize>>,
    next: AtomicU64,
    created: AtomicU64,
    /// The spec new pods are started with.
    desired: Mutex<String>,
    /// Replacements that fail to start (each failing one start).
    fail_starts: AtomicU64,
    /// Units reported as fallen back, with why.
    fallbacks: Mutex<Vec<(String, String)>>,
    /// The next replacement's resume fails with this.
    fail_resume: Mutex<Option<String>>,
    /// The next replacement resumes only this long after the commit.
    resume_after: Mutex<Option<std::time::Duration>>,
    /// Adoptions that fail (each failing one).
    fail_adoptions: AtomicU64,
    /// unit → when its view count last went to 0 (unix seconds).
    idle: Mutex<BTreeMap<String, u64>>,
    /// How the node is being drained.
    drain: Mutex<Drain>,
    /// Units whose identity is aside, since when.
    aside: Mutex<BTreeMap<String, u64>>,
    /// Units whose identity was forgotten, in order.
    forgotten: Mutex<Vec<String>>,
    /// Units with a fresh identity in place while an older one is aside.
    fresh: Mutex<std::collections::BTreeSet<String>>,
    /// Units whose superseded aside identity was retired, in order.
    discarded: Mutex<Vec<String>>,
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl InMemoryNodeEngines {
    pub fn new(node: &str) -> InMemoryNodeEngines {
        InMemoryNodeEngines {
            node: node.to_string(),
            any_path: false,
            engines: Mutex::default(),
            views: Mutex::default(),
            next: AtomicU64::new(1),
            created: AtomicU64::new(0),
            desired: Mutex::new("spec-1".into()),
            fail_starts: AtomicU64::new(0),
            fallbacks: Mutex::default(),
            fail_resume: Mutex::default(),
            resume_after: Mutex::default(),
            fail_adoptions: AtomicU64::new(0),
            idle: Mutex::default(),
            drain: Mutex::new(Drain::No),
            aside: Mutex::default(),
            forgotten: Mutex::default(),
            fresh: Mutex::default(),
            discarded: Mutex::default(),
        }
    }

    /// The node is (or is no longer) cordoned for a drain.
    pub fn set_draining(&self, draining: bool) {
        self.set_drain(if draining { Drain::Cordoned } else { Drain::No });
    }

    pub fn set_drain(&self, drain: Drain) {
        *self.drain.lock().unwrap() = drain;
    }

    /// The units whose identity is aside now.
    pub fn aside(&self) -> Vec<String> {
        self.aside.lock().unwrap().keys().cloned().collect()
    }

    /// A pod of `unit` started on a fresh identity while its old one is
    /// aside (the plugin and the pod died mid-collect; a stage came first).
    pub fn make_fresh_identity(&self, unit: &str) {
        self.fresh.lock().unwrap().insert(unit.to_string());
    }

    /// The units whose superseded aside identity was retired.
    pub fn discarded(&self) -> Vec<String> {
        self.discarded.lock().unwrap().clone()
    }

    /// The units whose identity [`NodeEngines::forget_unit`] retired.
    pub fn forgotten(&self) -> Vec<String> {
        self.forgotten.lock().unwrap().clone()
    }

    /// Whether `unit`'s pod is up.
    pub fn is_running(&self, unit: &str) -> bool {
        self.engines
            .lock()
            .unwrap()
            .get(unit)
            .is_some_and(|e| e.running)
    }

    /// The next replacement started resumes only `after` its commit.
    pub fn resume_next_standby_after(&self, after: std::time::Duration) {
        *self.resume_after.lock().unwrap() = Some(after);
    }

    /// The next `n` adoptions of a replacement fail (an API hiccup).
    pub fn fail_adoptions(&self, n: u64) {
        self.fail_adoptions.store(n, Ordering::SeqCst);
    }

    /// From now on new pods start from `spec`: the running ones drift.
    pub fn set_desired(&self, spec: &str) {
        *self.desired.lock().unwrap() = spec.to_string();
    }

    /// The spec `unit`'s pod runs.
    pub fn spec_of(&self, unit: &str) -> Option<String> {
        self.engines
            .lock()
            .unwrap()
            .get(unit)
            .map(|e| e.spec.clone())
    }

    /// The next `n` replacements fail to start.
    pub fn fail_replacement_starts(&self, n: u64) {
        self.fail_starts.store(n, Ordering::SeqCst);
    }

    /// `unit`'s replacement while it is a standby (a test injects faults
    /// into it).
    pub fn standby(&self, unit: &str) -> Option<Arc<InMemoryControl>> {
        self.engines
            .lock()
            .unwrap()
            .get(unit)
            .and_then(|e| e.standby.clone())
    }

    /// The next replacement started fails to resume, with `why`.
    pub fn fail_resume_of_next_standby(&self, why: &str) {
        *self.fail_resume.lock().unwrap() = Some(why.to_string());
    }

    /// The fallbacks reported, `(unit, why)`.
    pub fn fallbacks(&self) -> Vec<(String, String)> {
        self.fallbacks.lock().unwrap().clone()
    }

    /// For `--in-memory-backend` (see `any_path`).
    pub fn accepting_any_path(node: &str) -> InMemoryNodeEngines {
        InMemoryNodeEngines {
            any_path: true,
            ..InMemoryNodeEngines::new(node)
        }
    }

    /// Bring up `pool`'s engine serving `fs_uuid` ahead of time, so a test
    /// can plant volumes in it.
    pub fn plant(&self, pool: &PoolRef, fs_uuid: &str) -> Arc<InMemoryControl> {
        let (unit, _) = self.names(pool);
        self.get_or_start(&unit, pool, fs_uuid)
    }

    fn get_or_start(&self, unit: &str, pool: &PoolRef, fs_uuid: &str) -> Arc<InMemoryControl> {
        let mut engines = self.engines.lock().unwrap();
        if let Some(engine) = engines.get_mut(unit).filter(|e| !e.running) {
            // A new pod of a forgotten unit: a new node on the same tree.
            self.created.fetch_add(1, Ordering::SeqCst);
            engine.running = true;
            engine.started_at = unix_now();
            engine.incarnation = self.next.fetch_add(1, Ordering::SeqCst);
            engine.unlocked = None;
            if engine.awaits {
                engine.control.gate();
            }
        }
        let engine = engines.entry(unit.to_string()).or_insert_with(|| {
            self.created.fetch_add(1, Ordering::SeqCst);
            let control = InMemoryControl::serving(fs_uuid);
            if self.any_path {
                control.accept_any_path();
            }
            let awaits = pool.class.awaits_unlock();
            if awaits {
                control.gate();
            }
            FakeEngine {
                incarnation: self.next.fetch_add(1, Ordering::SeqCst),
                control: Arc::new(control),
                spec: self.desired.lock().unwrap().clone(),
                standby: None,
                awaits,
                target: unlock_target(pool),
                unlocked: None,
                running: true,
                started_at: unix_now(),
            }
        });
        engine.control.clone()
    }

    /// The fake's `fs.unlock` (the real one's rules: once per incarnation,
    /// again for other credentials).
    async fn unlock(&self, unit: &str, secrets: &Secrets) -> Result<bool, ControlError> {
        let (control, params, key) = {
            let engines = self.engines.lock().unwrap();
            let Some(engine) = engines.get(unit).filter(|e| e.awaits) else {
                return Ok(false);
            };
            let key = (engine.incarnation, fingerprint(secrets));
            if engine.unlocked == Some(key) {
                return Ok(true);
            }
            let Some(params) = unlock_params(&engine.target, secrets) else {
                return Ok(false);
            };
            (engine.control.clone(), params, key)
        };
        control.fs_unlock(params).await?;
        if let Some(engine) = self.engines.lock().unwrap().get_mut(unit) {
            engine.unlocked = Some(key);
        }
        Ok(true)
    }

    /// The engine of `unit`, if one runs.
    pub fn control(&self, unit: &str) -> Option<Arc<InMemoryControl>> {
        self.engines
            .lock()
            .unwrap()
            .get(unit)
            .map(|e| e.control.clone())
    }

    /// The pod of `unit` dies. Its filesystem does not: the next pod
    /// serves the same tree (a new [`InMemoryControl`] would lose it), so
    /// the tree is carried over and only the views go.
    pub fn crash(&self, unit: &str) {
        let mut engines = self.engines.lock().unwrap();
        if let Some(engine) = engines.get_mut(unit) {
            engine.incarnation = self.next.fetch_add(1, Ordering::SeqCst);
            engine.control.drop_views();
            if engine.awaits {
                // A new process: an empty EphemeralSecretStore.
                engine.control.gate();
            }
        }
    }

    /// The view count last recorded for `unit`.
    pub fn view_count(&self, unit: &str) -> Option<usize> {
        self.views.lock().unwrap().get(unit).copied()
    }

    /// How many engine pods were ever started.
    pub fn started(&self) -> u64 {
        self.created.load(Ordering::SeqCst)
    }

    fn connect(&self, unit: &str) -> Option<NodeEngine> {
        let engines = self.engines.lock().unwrap();
        let engine = engines.get(unit).filter(|e| e.running)?;
        Some(NodeEngine {
            client: engine.control.clone(),
            unit: unit.to_string(),
            pod: format!("constellation-engine-{unit}-{}", self.node),
            incarnation: engine.incarnation.to_string(),
        })
    }
}

#[async_trait]
impl NodeEngines for InMemoryNodeEngines {
    fn names(&self, pool: &PoolRef) -> (String, String) {
        let unit = unit_name(pool);
        let pod = format!("constellation-engine-{unit}-{}", self.node);
        (unit, pod)
    }

    async fn engine(
        &self,
        pool: &PoolRef,
        fs_uuid: &str,
        unlock: Option<&Secrets>,
    ) -> Result<NodeEngine, ControlError> {
        let (unit, _) = self.names(pool);
        self.get_or_start(&unit, pool, fs_uuid);
        if let Some(secrets) = unlock {
            self.unlock(&unit, secrets).await?;
        }
        Ok(self.connect(&unit).expect("just started"))
    }

    async fn rotate(&self, unit: &str, secrets: &Secrets) -> Result<bool, ControlError> {
        self.unlock(unit, secrets).await
    }

    async fn existing(&self, unit: &str) -> Result<Option<NodeEngine>, ControlError> {
        Ok(self.connect(unit))
    }

    async fn set_view_count(&self, unit: &str, views: usize) -> Result<(), ControlError> {
        self.views.lock().unwrap().insert(unit.to_string(), views);
        let mut idle = self.idle.lock().unwrap();
        if views == 0 {
            idle.insert(unit.to_string(), unix_now());
        } else {
            idle.remove(unit);
        }
        Ok(())
    }

    async fn units(&self) -> Result<Vec<UnitPod>, ControlError> {
        let views = self.views.lock().unwrap().clone();
        let idle = self.idle.lock().unwrap().clone();
        Ok(self
            .engines
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, e)| e.running)
            .map(|(unit, e)| UnitPod {
                unit: unit.clone(),
                pod: format!("constellation-engine-{unit}-{}", self.node),
                ready: true,
                views: views.get(unit).map(|v| *v as u64),
                idle_since: idle.get(unit).copied(),
                created: Some(e.started_at),
            })
            .collect())
    }

    async fn draining(&self) -> Result<Drain, ControlError> {
        Ok(*self.drain.lock().unwrap())
    }

    async fn set_identity_aside(&self, unit: &str) -> Result<(), ControlError> {
        self.aside
            .lock()
            .unwrap()
            .entry(unit.to_string())
            .or_insert_with(unix_now);
        Ok(())
    }

    async fn restore_identity(&self, unit: &str) -> Result<(), ControlError> {
        if self.fresh.lock().unwrap().contains(unit) {
            return Err(ControlError::failed(format!(
                "a fresh identity of {unit} is in place"
            )));
        }
        self.aside.lock().unwrap().remove(unit);
        Ok(())
    }

    async fn discard_superseded_aside(&self, unit: &str) -> Result<bool, ControlError> {
        if !self.fresh.lock().unwrap().contains(unit)
            || self.aside.lock().unwrap().remove(unit).is_none()
        {
            return Ok(false);
        }
        self.discarded.lock().unwrap().push(unit.to_string());
        Ok(true)
    }

    async fn identities_aside(&self) -> Result<Vec<(String, u64)>, ControlError> {
        Ok(self
            .aside
            .lock()
            .unwrap()
            .iter()
            .map(|(u, t)| (u.clone(), *t))
            .collect())
    }

    async fn forget_unit(&self, unit: &str) -> Result<(), ControlError> {
        if let Some(engine) = self.engines.lock().unwrap().get_mut(unit) {
            engine.running = false;
            engine.standby = None;
            engine.control.drop_views();
        }
        self.views.lock().unwrap().remove(unit);
        self.idle.lock().unwrap().remove(unit);
        self.aside.lock().unwrap().remove(unit);
        self.forgotten.lock().unwrap().push(unit.to_string());
        Ok(())
    }

    async fn drifted(&self) -> Result<Vec<Drift>, ControlError> {
        let desired = self.desired.lock().unwrap().clone();
        Ok(self
            .engines
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, e)| e.spec != desired)
            .map(|(unit, e)| Drift {
                unit: unit.clone(),
                pod: format!("constellation-engine-{unit}-{}", self.node),
                desired: desired.clone(),
                why: format!("spec {} -> {desired}", e.spec),
            })
            .collect())
    }

    async fn start_replacement(&self, unit: &str) -> Result<Replacement, ControlError> {
        if self
            .fail_starts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(ControlError::unavailable(
                "injected: the replacement did not start",
            ));
        }
        let mut engines = self.engines.lock().unwrap();
        let engine = engines
            .get_mut(unit)
            .ok_or_else(|| ControlError::not_found(format!("no engine pod for {unit}")))?;
        let standby = Arc::new(InMemoryControl::standby_for(&engine.control));
        if engine.awaits {
            // `--await-unlock`: only the handoff's `Credentials` step
            // unlocks it.
            standby.gate();
        }
        if let Some(why) = self.fail_resume.lock().unwrap().take() {
            standby.fail_resume(&why);
        }
        if let Some(after) = self.resume_after.lock().unwrap().take() {
            standby.resume_after(after);
        }
        engine.standby = Some(standby.clone());
        Ok(Replacement {
            pod: format!("constellation-engine-{unit}-{}-next", self.node),
            handoff: standby,
        })
    }

    async fn adopt_replacement(
        &self,
        unit: &str,
        _replacement: &Replacement,
    ) -> Result<NodeEngine, ControlError> {
        if self
            .fail_adoptions
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(ControlError::unavailable(
                "injected: the replacement is not ready yet",
            ));
        }
        {
            let mut engines = self.engines.lock().unwrap();
            let engine = engines
                .get_mut(unit)
                .ok_or_else(|| ControlError::not_found(format!("no engine pod for {unit}")))?;
            let standby = engine
                .standby
                .take()
                .ok_or_else(|| ControlError::not_found("no replacement to adopt"))?;
            engine.control = standby;
            engine.incarnation = self.next.fetch_add(1, Ordering::SeqCst);
            engine.spec = self.desired.lock().unwrap().clone();
        }
        Ok(self.connect(unit).expect("adopted"))
    }

    async fn discard_replacement(&self, unit: &str, _replacement: Replacement) {
        if let Some(engine) = self.engines.lock().unwrap().get_mut(unit) {
            engine.standby = None;
        }
    }

    /// The next pod serves the same tree (as [`Self::crash`]), from the
    /// desired spec, with no view.
    async fn retire(&self, unit: &str) -> Result<(), ControlError> {
        let desired = self.desired.lock().unwrap().clone();
        if let Some(engine) = self.engines.lock().unwrap().get_mut(unit) {
            if let Some(standby) = engine.standby.take() {
                engine.control = standby;
            }
            engine.control.drop_views();
            if engine.awaits {
                engine.control.gate();
            }
            engine.incarnation = self.next.fetch_add(1, Ordering::SeqCst);
            engine.spec = desired;
        }
        Ok(())
    }

    async fn report_fallback(&self, unit: &str, why: &str) {
        self.fallbacks
            .lock()
            .unwrap()
            .push((unit.to_string(), why.to_string()));
    }
}
