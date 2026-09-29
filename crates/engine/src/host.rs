//! [`EngineHost`]: N engines in one process, within one [`ResourceBudget`]
//! (plan 31 §4.1).
//!
//! Today's daemon is one host with one engine (plan 21: one daemon per
//! state dir, i.e. per (bucket, prefix) and node, serving every view of
//! it); a dense server or a CSI engine pod may host several filesystems.
//! Every engine of a host shares its one tokio runtime (one runtime per
//! engine would multiply thread pools for nothing: the ordering and
//! admission barriers are per view, not per runtime).
//!
//! **Partitioning.** The host's budget is split equally: the engine added
//! as the n-th gets `budget / n` of memory, cache and staging
//! ([`ResourceBudget::share`]), unless its [`crate::EngineProfile`]
//! overrides memory or cache explicitly (§10.1's `Server` knob); an
//! engine never gets more than its own `EngineConfig` asks for. Shares
//! are fixed when an engine starts: adding one does not shrink the ones
//! already running (their caches are open at their size), and removing
//! one does not grow the rest — rebalancing a live cache is plan 31 C8's
//! lifecycle work. A single-engine host therefore behaves exactly as a
//! bare engine with the same numbers.

use crate::node::{Engine, EngineConfig};
use crate::profile::EngineProfile;
use anyhow::{bail, Result};
use constellation_platform::HostServices;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

/// Which filesystem an engine of a host serves (its registry name, or its
/// state dir for an unregistered one).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FsId(pub String);

impl FsId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for FsId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A host-wide ceiling, partitioned across its engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceBudget {
    pub memory_bytes: u64,
    pub cache_bytes: u64,
    pub staging_bytes: u64,
}

impl ResourceBudget {
    /// No ceiling (a standalone engine).
    pub const fn unlimited() -> Self {
        Self {
            memory_bytes: u64::MAX,
            cache_bytes: u64::MAX,
            staging_bytes: u64::MAX,
        }
    }

    /// One engine's equal share when `engines` share this budget.
    pub fn share(&self, engines: usize) -> Self {
        let n = engines.max(1) as u64;
        let part = |total: u64| {
            if total == u64::MAX {
                u64::MAX
            } else {
                total / n
            }
        };
        Self {
            memory_bytes: part(self.memory_bytes),
            cache_bytes: part(self.cache_bytes),
            staging_bytes: part(self.staging_bytes),
        }
    }
}

/// The engines of one process (see the module doc).
pub struct EngineHost {
    runtime: tokio::runtime::Handle,
    budget: Arc<ResourceBudget>,
    engines: RwLock<BTreeMap<FsId, Arc<Engine>>>,
    /// Serialises `add_engine`s, so two starting at once see each
    /// other's share.
    adding: Mutex<()>,
}

impl EngineHost {
    pub fn start(runtime: tokio::runtime::Handle, budget: ResourceBudget) -> EngineHost {
        EngineHost {
            runtime,
            budget: Arc::new(budget),
            engines: RwLock::new(BTreeMap::new()),
            adding: Mutex::new(()),
        }
    }

    pub fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }

    pub fn budget(&self) -> &Arc<ResourceBudget> {
        &self.budget
    }

    /// What the next engine added with `profile` would run within.
    pub fn allotment_for(&self, profile: &EngineProfile) -> ResourceBudget {
        let n = self.engines.read().unwrap().len() + 1;
        let mut share = self.budget.share(n);
        if let Some(memory) = profile.memory_budget {
            share.memory_bytes = memory;
        }
        if let Some(cache) = profile.cache_budget {
            share.cache_bytes = cache;
        }
        share
    }

    /// Start an engine for `id` on this host's runtime, within its share
    /// of the budget. Blocks while the engine starts (call it from a
    /// thread that is not one of the runtime's workers).
    pub fn add_engine(
        &self,
        id: FsId,
        mut cfg: EngineConfig,
        host: HostServices,
        profile: EngineProfile,
    ) -> Result<Arc<Engine>> {
        let _adding = self.adding.lock().unwrap();
        if self.engines.read().unwrap().contains_key(&id) {
            bail!("an engine for {id} is already running in this process");
        }
        let allotment = self.allotment_for(&profile);
        cfg.runtime = Some(self.runtime.clone());
        let engine = Arc::new(Engine::start_with(cfg, host, profile, allotment)?);
        self.engines.write().unwrap().insert(id, engine.clone());
        Ok(engine)
    }

    /// Take `id`'s engine off the host (its views closed first, by the
    /// caller; `node.leave` shuts the engine down after).
    pub fn remove_engine(&self, id: &FsId) -> Option<Arc<Engine>> {
        self.engines.write().unwrap().remove(id)
    }

    pub fn engine(&self, id: &FsId) -> Option<Arc<Engine>> {
        self.engines.read().unwrap().get(id).cloned()
    }

    /// Every engine, by id.
    pub fn engines(&self) -> Vec<(FsId, Arc<Engine>)> {
        self.engines
            .read()
            .unwrap()
            .iter()
            .map(|(id, e)| (id.clone(), e.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_budget_is_shared_equally_and_unlimited_stays_unlimited() {
        let budget = ResourceBudget {
            memory_bytes: 12 << 30,
            cache_bytes: 90 << 30,
            staging_bytes: 3 << 30,
        };
        assert_eq!(budget.share(1), budget);
        assert_eq!(
            budget.share(0),
            budget,
            "no engines yet: the first gets all"
        );
        assert_eq!(
            budget.share(3),
            ResourceBudget {
                memory_bytes: 4 << 30,
                cache_bytes: 30 << 30,
                staging_bytes: 1 << 30,
            }
        );
        assert_eq!(
            ResourceBudget::unlimited().share(7),
            ResourceBudget::unlimited()
        );
    }

    #[test]
    fn the_next_engines_allotment_is_its_share_or_its_profiles_override() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let host = EngineHost::start(
            rt.handle().clone(),
            ResourceBudget {
                memory_bytes: 8 << 30,
                cache_bytes: 64 << 30,
                staging_bytes: 4 << 30,
            },
        );
        let desktop = EngineProfile::desktop();
        assert_eq!(host.allotment_for(&desktop).cache_bytes, 64 << 30);
        let server = EngineProfile::server(1 << 30, 5 << 30);
        let allot = host.allotment_for(&server);
        assert_eq!((allot.memory_bytes, allot.cache_bytes), (1 << 30, 5 << 30));
        assert_eq!(allot.staging_bytes, 4 << 30, "staging is always the share");
        assert!(host.engines().is_empty());
        assert!(host.engine(&FsId::new("nope")).is_none());
        assert!(host.remove_engine(&FsId::new("nope")).is_none());
    }
}
