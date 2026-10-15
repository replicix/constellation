//! `export` in its two senses, and `import`.
//!
//! - [`Manager::export`] is the CLI's `constellation export NAME` (plan 21
//!   step 6), `zpool export`: the only way to un-register a name — leave
//!   the cluster, delete the state dir, drop the row. It is not a control
//!   method: the daemon that would answer it is the one being torn down.
//! - [`Manager::export_document`] / [`Manager::import`] are `fs.export` /
//!   `fs.import` (plan 31 §9.2): a name's registry row as a self-contained
//!   TOML document, and registering one from it on another host. Reading
//!   it changes nothing.

use super::{failed, Manager, Retext};
use crate::registry::{self, FsEntry, FsOverrides};
use constellation_control::proto::ControlError;
use constellation_meta::Meta;
use constellation_types::Code;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How [`Manager::export`] reaches the daemon serving a state dir: the
/// CLI's control client (`constellation_control::Client` over the socket
/// recorded in the state dir), a fake in tests.
pub trait Daemons: Send + Sync {
    /// Whether the daemon of `state_dir` is alive *and answering*
    /// (bounded: a wedged one that keeps its listener is not).
    fn answers<'a>(&'a self, state_dir: &'a Path) -> BoxFuture<'a, bool>;

    /// `node.leave{node_id: None, force}` on the daemon of `state_dir`:
    /// `Ok(Ok(detail))` it left, `Ok(Err(refusal))` it refused, `Err` it
    /// could not be reached.
    fn leave<'a>(
        &'a self,
        state_dir: &'a Path,
        force: bool,
    ) -> BoxFuture<'a, Result<Result<String, ControlError>, ControlError>>;

    /// Whether the daemon of `state_dir` still has its socket (it removes
    /// it only at the very end of a clean shutdown).
    fn socket_exists(&self, state_dir: &Path) -> bool;
}

/// What [`Manager::export`] did on the way, for its caller to report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportNote {
    /// The daemon left the cluster (its answer).
    Left(String),
    /// The daemon refused to leave; `--force` continues.
    LeaveRefused(String),
    /// The daemon was unreachable; its node was retired from the store.
    Retired(u64),
}

/// A self-contained registry document (`fs.export` → `fs.import`).
#[derive(Debug, Serialize, Deserialize)]
pub struct RegistryDocument {
    pub name: String,
    #[serde(flatten)]
    pub entry: FsEntry,
}

/// How long `export` waits for a daemon that left to close its socket.
const EXIT_WAIT: Duration = Duration::from_secs(30);

impl Manager {
    /// `constellation export NAME` (module docs). Under the registry's lock
    /// throughout, so nothing re-registers the name half way:
    ///
    /// 1. never mounted (no `node_id` claimed): skip to 4;
    /// 2. the daemon answers: `node.leave` — drains the journal, releases
    ///    leases, tombstones the node, detaches every view — then wait for
    ///    it to close its socket (`force`: do not wait; a refusal is
    ///    reported and passed over);
    /// 3. the daemon is unreachable: refuse while `daemon.lock` is held (a
    ///    daemon starting up or wedged, not gone) unless `force`, else
    ///    retire the node directly against the store (no journal to drain
    ///    from a dead process: lease expiry and replica recovery cover the
    ///    unshipped tail);
    /// 4. delete the state dir — never one holding another name's state
    ///    or cache dir, the registry, or the host's data, config or runtime
    ///    dir (or when one of those cannot be located), refused before
    ///    step 2 — and the row.
    pub async fn export(
        &self,
        name: &str,
        force: bool,
        daemons: &dyn Daemons,
        notes: &mut (dyn FnMut(ExportNote) + Send),
    ) -> Result<(), ControlError> {
        let mut reg = self.registry_locked()?;
        let entry = reg.entry(name).cloned().ok_or_else(|| {
            ControlError::not_found(format!("{name:?} is not a registered filesystem"))
        })?;
        let dirs = &self.host.dirs;
        let host_dirs = [
            ("the data dir", dirs.data_dir()),
            ("the config dir", dirs.config_dir()),
            ("the runtime dir", dirs.runtime_dir()),
        ];
        reg.check_deletable(name, &host_dirs)
            .map_err(|e| ControlError::denied(format!("{e:#}")))?;
        let dir = entry.state_dir.clone();
        let db_path = dir.join("meta.db");
        // A running daemon holds `fjall`'s single-process lock on
        // `meta.db`, so ask it first: if it answers at all it has claimed a
        // node id, and opening the store here would fail `Locked` and skip
        // the leave below.
        let alive = daemons.answers(&dir).await;
        let node_id_claimed = alive
            || (db_path.exists()
                && Meta::open(&db_path)
                    .ok()
                    .and_then(|m| m.kv_get("node_id").ok().flatten())
                    .is_some());
        if node_id_claimed {
            match daemons.leave(&dir, force).await {
                Ok(Ok(detail)) => {
                    notes(ExportNote::Left(detail));
                    let deadline = Instant::now() + EXIT_WAIT;
                    while !force && daemons.socket_exists(&dir) && Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
                Ok(Err(refusal)) => {
                    if !force {
                        return Err(ControlError::failed(format!(
                            "leave failed: {}",
                            refusal.message
                        )));
                    }
                    notes(ExportNote::LeaveRefused(refusal.message));
                }
                Err(_) => {
                    if !force && registry::daemon_lock_held(&dir) {
                        return Err(ControlError::from(Code::Busy).retext(format!(
                            "daemon.lock for {name:?} is held by a live process (starting up \
                             or wedged, not gone); pass --force to retire it anyway"
                        )));
                    }
                    let node_id = self.retire(&entry, &db_path).await?;
                    notes(ExportNote::Retired(node_id));
                }
            }
        }
        if dir.exists() {
            std::fs::remove_dir_all(&dir)
                .map_err(|e| failed(format!("removing {}: {e}", dir.display())))?;
        }
        reg.remove(name).map_err(failed)
    }

    /// Retire the node a dead daemon claimed in `db_path`, with the admin
    /// path run directly against the store.
    async fn retire(&self, entry: &FsEntry, db_path: &Path) -> Result<u64, ControlError> {
        let meta = Arc::new(Meta::open(db_path).map_err(failed)?);
        let node_id: u64 = meta
            .kv_get("node_id")
            .map_err(failed)?
            .ok_or_else(|| ControlError::failed("no node_id on record"))?
            .parse()
            .map_err(failed)?;
        let (store, _) = self.open(&entry.s3, None).await?;
        let designations = crate::designation::DesignationManager::new(
            constellation_store_s3::designation::DesignationStore::new(
                store.inner().clone(),
                constellation_store_s3::designation::DesignationMode::Cas,
            ),
            meta.clone(),
            constellation_net::Peers::disabled(),
            node_id,
        );
        // `self_id` only guards "you can't admin-leave yourself via the
        // admin form"; nothing here is a mounted node, so `0` — never a
        // claimed node id — just needs to differ from `node_id`.
        crate::leave::admin_leave(store.inner().clone(), &designations, 0, node_id, true)
            .await
            .map_err(|e| failed(format!("admin-retiring the dead daemon's node: {e}")))?;
        Ok(node_id)
    }

    /// `fs.export`: `name`'s registry row as a TOML document.
    pub fn export_document(&self, name: &str) -> Result<String, ControlError> {
        let entry = self
            .entry(name)?
            .ok_or_else(|| ControlError::not_found(format!("{name:?} is not registered")))?;
        toml::to_string(&RegistryDocument {
            name: name.to_string(),
            entry,
        })
        .map_err(failed)
    }

    /// `fs.import`: register `document` (an [`Manager::export_document`])
    /// under `name` (else the document's). The state dir is this host's
    /// (the name's, or the default), never the document's. A name already
    /// registered at another URL is refused, in use or not: an import
    /// brings a name over, it does not repoint one.
    pub fn import(
        &self,
        document: &str,
        name: Option<String>,
    ) -> Result<(String, FsEntry), ControlError> {
        let doc: RegistryDocument = toml::from_str(document)
            .map_err(|e| ControlError::invalid(format!("the document: {e}")))?;
        let name = name.unwrap_or(doc.name);
        registry::validate_name(&name).map_err(|e| ControlError::invalid(format!("{e:#}")))?;
        let e = doc.entry;
        let mut registry = self.registry_locked()?;
        if let Some(existing) = registry.entry(&name) {
            if existing.s3 != e.s3 {
                return Err(ControlError::from(Code::Exists)
                    .with_details(serde_json::json!({ "name": name, "s3": existing.s3 })));
            }
        }
        let opt = |s: &str| (!s.is_empty()).then(|| s.to_string());
        let mut entry = registry
            .merge_and_save(
                &name,
                FsOverrides {
                    s3: Some(e.s3.clone()),
                    state_dir: None,
                    cache_size: opt(&e.cache_size),
                    cache_dir: opt(&e.cache_dir),
                    fsync_mode: opt(&e.fsync_mode),
                    write_mode: opt(&e.write_mode),
                    read_only_member: Some(e.read_only_member),
                    web_ui: Some(e.web_ui),
                    endpoint: opt(&e.endpoint),
                    mount: None,
                },
            )
            .map_err(failed)?;
        for m in e.mounts {
            entry = registry
                .merge_and_save(
                    &name,
                    FsOverrides {
                        mount: Some(m),
                        ..Default::default()
                    },
                )
                .map_err(failed)?;
        }
        Ok((name, entry))
    }
}
