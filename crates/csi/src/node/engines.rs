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
use crate::engine_pods::unit_name;
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
    /// pod serves it). `reads_secret`: the pod reads the unit's credentials
    /// `Secret`, refreshed from `pool.secrets` when they are given.
    async fn engine(
        &self,
        pool: &PoolRef,
        fs_uuid: &str,
        reads_secret: bool,
    ) -> Result<NodeEngine, ControlError>;

    /// The engine pod of `unit` when it is running and ready; `None` when
    /// it is not (gone, or not ready yet). Never creates one.
    async fn existing(&self, unit: &str) -> Result<Option<NodeEngine>, ControlError>;

    /// Record how many staged volumes the pod of `unit` serves (§7's
    /// `last-view-count` annotation, and since when it has served none: the
    /// idle GC of 37-k6b reads them).
    async fn set_view_count(&self, unit: &str, views: usize) -> Result<(), ControlError>;
}

struct FakeEngine {
    incarnation: u64,
    control: Arc<InMemoryControl>,
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
        }
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
        self.get_or_start(&unit, fs_uuid)
    }

    fn get_or_start(&self, unit: &str, fs_uuid: &str) -> Arc<InMemoryControl> {
        let mut engines = self.engines.lock().unwrap();
        let engine = engines.entry(unit.to_string()).or_insert_with(|| {
            self.created.fetch_add(1, Ordering::SeqCst);
            let control = InMemoryControl::serving(fs_uuid);
            if self.any_path {
                control.accept_any_path();
            }
            FakeEngine {
                incarnation: self.next.fetch_add(1, Ordering::SeqCst),
                control: Arc::new(control),
            }
        });
        engine.control.clone()
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
        let engine = engines.get(unit)?;
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
        _reads_secret: bool,
    ) -> Result<NodeEngine, ControlError> {
        let (unit, _) = self.names(pool);
        self.get_or_start(&unit, fs_uuid);
        Ok(self.connect(&unit).expect("just started"))
    }

    async fn existing(&self, unit: &str) -> Result<Option<NodeEngine>, ControlError> {
        Ok(self.connect(unit))
    }

    async fn set_view_count(&self, unit: &str, views: usize) -> Result<(), ControlError> {
        self.views.lock().unwrap().insert(unit.to_string(), views);
        Ok(())
    }
}
