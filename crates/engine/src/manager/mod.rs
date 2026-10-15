//! The filesystem `Manager` (plan 31 §4): the one implementation of
//! filesystem management — the registry of named filesystems, and
//! `create`, `import`, `export`, `passwd`, `doctor`, `list` — that the CLI
//! calls in-process and the control service serves as `fs.*` (§9.2, §9.7).
//!
//! ## Who calls it
//!
//! - **The CLI** (`fs create`, `fs passwd`, `fs list`, `doctor`, `export`,
//!   `mount`'s registry write, and every `NAME[:path]` it resolves) is a
//!   thin wrapper: argument parsing, the passphrase prompts, the output.
//! - **The control service** (`engine::control`'s `fs.*`) maps the
//!   protocol's parameters onto the same calls, signing with what
//!   `fs.unlock` supplied ([`credentials`]).
//! - **`constellation serve --create`** (an engine pod's first start) makes
//!   its filesystem through [`Manager::create`] with no name.
//!
//! What differs between them is an input, never a second copy of a rule:
//! credentials (`None` is the environment's AWS chain), how a passphrase is
//! obtained ([`CreateHooks`], [`Manager::passwd`]'s `secrets`), what an
//! existing filesystem at the URL means ([`Existing`]), how long a
//! `meta.json` read may wait for a store that disagrees with itself
//! ([`Manager::with_meta_wait`]: an interactive command waits as long as
//! the store's default, a control call two seconds), and how the daemon of
//! a state dir is reached ([`Daemons`], the CLI's control client).
//!
//! ## The rules it owns
//!
//! - Names are single path components ([`registry::validate_name`]).
//! - Identity is pinned to the name: a name's URL changes only while its
//!   state dir is not in use ([`Registry::check_repoint`], plan 21 — for
//!   `mount --s3`, `fs create` and `fs.create` alike; `fs.import` refuses
//!   any other URL), and its state dir moves only to a new or empty dir
//!   nobody else has ([`Registry::explicit_state_dir`]). Both are checked
//!   before anything is written and again under the registry's lock, which
//!   a named create holds across its conditional `PUT` and until the name
//!   is registered. "In use" is a `meta.db` or a held `daemon.lock`, which
//!   `mount` takes before it lets the registry's lock go (the CLI's
//!   `cmd_mount`), so a name is in use from its mount's registration on.
//! - A new filesystem is created only after the backend passed the
//!   preflight, by one conditional `PUT` of `meta.json` that carries the
//!   E2E keyring, with `gossip_secret` kept out of `meta.json` for an E2E
//!   filesystem (its gossip topic is seeded from the keyring), and the
//!   name's E2E pin recorded.
//! - `meta.json`'s existence is asked with a `HEAD` before it is ever read:
//!   a `GET` of a key that does not exist yet is what a store caching
//!   negative answers keeps serving to the `mount` after the create.
//! - A passphrase change holds the master key against this host's E2E pin
//!   before it rewraps anything ([`crate::e2e_pin`]), and rewraps exactly
//!   the keyring block it checked (the update is conditional on it).
//! - `export` deletes a state dir only after the node left the cluster (or
//!   was retired), and never one that holds another name's state or cache
//!   dir, the registry, or the host's data, config or runtime dir — nor
//!   when one of those cannot be located.
//!
//! Errors are [`ControlError`]s: the portable `Code`, a message, and
//! details, which the control service answers as they are and the CLI
//! prints by their message.

mod create;
pub mod credentials;
mod doctor;
mod export;
mod passwd;

#[cfg(test)]
mod tests;

pub use create::{CreateHooks, CreateRequest, Created, Existing};
pub use doctor::{DoctorReport, FsState};
pub use export::{Daemons, ExportNote, RegistryDocument};
pub use passwd::{PasswdOutcome, PasswdRequest};

use crate::backend;
use crate::e2e_pin::PinStore;
use crate::registry::{self, FsEntry, FsOverrides, Registry};
use crate::target::Target;
use constellation_control::proto::ControlError;
use constellation_platform::{CredentialSource, HostServices};
use constellation_store_s3::{ChunkStore, StoreError};
use constellation_types::Code;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// The filesystem manager. See the module docs.
#[derive(Clone)]
pub struct Manager {
    host: HostServices,
    /// `None`: `$CONSTELLATION_REGISTRY`, else the host's config dir,
    /// resolved on every use (as the registry always was).
    registry: Option<PathBuf>,
    /// How long a `meta.json` read waits for a `GET` that disagrees with
    /// its `HEAD` (`None`: the store's default, `load_fs`).
    meta_wait: Option<Duration>,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("registry", &self.registry)
            .field("meta_wait", &self.meta_wait)
            .finish_non_exhaustive()
    }
}

/// `e` as a `Failed` control error, its whole chain in the message.
pub(crate) fn failed(e: impl std::fmt::Display) -> ControlError {
    ControlError::failed(format!("{e:#}"))
}

/// A store refusal as the control error it stands for: `Unavailable`
/// when asking again may answer (`constellation_store_s3::classify`), so
/// a caller — the CSI plugin, `serve`'s credential gate — retries a flaky
/// S3 and gives up on a refusal.
pub(crate) fn store_err(e: StoreError) -> ControlError {
    match e {
        StoreError::NotFound => ControlError::not_found(e.to_string()),
        StoreError::AlreadyExists => ControlError::from(Code::Exists)
            .with_details(serde_json::json!({ "reason": e.to_string() })),
        other => store_failed(other, None),
    }
}

/// `e`, a store failure while `doing`, classified as [`store_err`] does.
pub(crate) fn store_failed(e: StoreError, doing: Option<&str>) -> ControlError {
    let transient = constellation_store_s3::classify(&e).is_transient();
    let message = match doing {
        Some(doing) => format!("{doing}: {e}"),
        None => e.to_string(),
    };
    match transient {
        true => ControlError::unavailable(message),
        false => ControlError::failed(message),
    }
}

/// [`Registry::check_repoint`]'s refusal, as `Exists` with the name's
/// current URL in the details.
fn repoint_err(r: registry::Repoint) -> ControlError {
    ControlError::from(Code::Exists)
        .with_details(serde_json::json!({
            "name": r.name,
            "s3": r.current,
        }))
        .retext(r.to_string())
        .with_remediation(format!(
            "run `constellation export {}` first, or use another name",
            r.name
        ))
}

impl Manager {
    /// The manager of `host`'s registry (`$CONSTELLATION_REGISTRY`, else
    /// `<config dir>/registry.toml`) and E2E pins.
    pub fn new(host: HostServices) -> Manager {
        Manager {
            host,
            registry: None,
            meta_wait: None,
        }
    }

    /// [`Manager::new`] on this process's native host services.
    pub fn native() -> Manager {
        Manager::new(constellation_platform::native().clone())
    }

    /// A manager of the registry at `path`, with its E2E pins beside it
    /// (`registry.e2e.toml`): tests, and embedders that keep their own.
    pub fn at(path: PathBuf, host: HostServices) -> Manager {
        Manager {
            host,
            registry: Some(path),
            meta_wait: None,
        }
    }

    /// Bound every `meta.json` read that finds a `HEAD`/`GET`
    /// disagreement to `wait` (a control call must answer; an interactive
    /// command may wait out the store's default).
    pub fn with_meta_wait(mut self, wait: Duration) -> Manager {
        self.meta_wait = Some(wait);
        self
    }

    pub fn host(&self) -> &HostServices {
        &self.host
    }

    /// Where the registry lives.
    pub fn registry_path(&self) -> Result<PathBuf, ControlError> {
        match &self.registry {
            Some(path) => Ok(path.clone()),
            None => Registry::path_in(&self.host).map_err(failed),
        }
    }

    /// The registry, read without its lock: a glance (`fs list`, name
    /// resolution) does not serialize against writers.
    pub fn registry(&self) -> Result<Registry, ControlError> {
        Ok(Registry::load_at(self.registry_path()?)
            .map_err(failed)?
            .with_dirs(self.host.dirs.clone()))
    }

    /// The registry under its exclusive lock, held until the value drops:
    /// a whole read-check-write cycle (`mount`'s registration).
    pub fn registry_locked(&self) -> Result<Registry, ControlError> {
        Ok(Registry::load_locked_at(self.registry_path()?)
            .map_err(failed)?
            .with_dirs(self.host.dirs.clone()))
    }

    /// The E2E pin store: beside an explicit registry, else the host's
    /// secret store (`e2e_pin::pins_store_in`).
    pub fn pins(&self) -> anyhow::Result<PinStore> {
        match &self.registry {
            Some(path) => PinStore::at(&path.with_extension("e2e.toml")),
            None => crate::e2e_pin::pins_store_in(&self.host),
        }
    }

    /// Every registered filesystem, by name.
    pub fn list(&self) -> Result<Vec<(String, FsEntry)>, ControlError> {
        Ok(self
            .registry()?
            .iter()
            .map(|(name, entry)| (name.clone(), entry.clone()))
            .collect())
    }

    /// `name`'s registry entry, if it is registered.
    pub fn entry(&self, name: &str) -> Result<Option<FsEntry>, ControlError> {
        Ok(self.registry()?.entry(name).cloned())
    }

    /// A CLI target (`myfs`, `myfs:/data`, `/data`) against the registry
    /// (plan 21 step 3, [`crate::target::resolve`]).
    pub fn resolve(&self, raw: &str) -> Result<Target, ControlError> {
        Ok(crate::target::resolve(raw, &self.registry()?))
    }

    /// `mount NAME`'s registry write, in `reg` (held locked by the caller
    /// across its resolution of the target): the identity rules for an
    /// explicit `--s3` ([`Registry::check_repoint`]) and `--state-dir`
    /// ([`Registry::explicit_state_dir`]), then `overrides` with them —
    /// a repointed name forgets the old URL's endpoint.
    pub fn save_mount(
        &self,
        reg: &mut Registry,
        name: &str,
        s3: Option<&str>,
        state_dir: Option<&Path>,
        mut overrides: FsOverrides,
    ) -> Result<FsEntry, ControlError> {
        registry::validate_name(name).map_err(|e| ControlError::invalid(format!("{e:#}")))?;
        let current = reg.entry(name).cloned().unwrap_or_default();
        if let Some(s3) = s3 {
            reg.check_repoint(name, s3).map_err(repoint_err)?;
            if s3 != current.s3 {
                overrides.endpoint = Some(String::new());
            }
        }
        overrides.s3 = s3.map(str::to_string);
        overrides.state_dir = state_dir
            .map(|dir| reg.explicit_state_dir(name, dir))
            .transpose()
            .map_err(|e| ControlError::from(Code::Exists).retext(format!("{e:#}")))?;
        reg.merge_and_save(name, overrides).map_err(failed)
    }

    /// Remember the endpoint `name`'s `meta.json` was read from (best
    /// effort: diagnostics only, see [`FsEntry::endpoint`]).
    pub fn remember_endpoint(&self, name: &str, endpoint: &str) {
        let saved = self.registry_locked().and_then(|mut reg| {
            // A name exported meanwhile stays exported.
            if reg.entry(name).is_none() {
                return Ok(());
            }
            reg.merge_and_save(
                name,
                FsOverrides {
                    endpoint: Some(endpoint.to_string()),
                    ..Default::default()
                },
            )
            .map(drop)
            .map_err(failed)
        });
        if let Err(e) = saved {
            tracing::debug!(name, error = %e.message, "recording the name's S3 endpoint failed");
        }
    }

    /// The store at `url`, signing with `credentials` (`None`: the
    /// environment's AWS chain), and where it resolved to.
    pub async fn open(
        &self,
        url: &str,
        credentials: Option<&Arc<CredentialSource>>,
    ) -> Result<(ChunkStore, backend::BackendInfo), ControlError> {
        let (store, info) = backend::open_backend_described_with(url, credentials)
            .await
            .map_err(|e| {
                let message = format!("opening backend: {e:#}");
                match constellation_store_s3::classify_chain(e.chain()).is_transient() {
                    true => ControlError::unavailable(message),
                    false => ControlError::failed(message),
                }
            })?;
        Ok((ChunkStore::new(store), info))
    }

    /// `meta.json` at `store`, waiting as this manager does.
    pub async fn load_meta(
        &self,
        store: &ChunkStore,
    ) -> Result<constellation_store_s3::FsMeta, StoreError> {
        match self.meta_wait {
            Some(wait) => store.load_fs_waiting(wait).await,
            None => store.load_fs().await,
        }
    }
}

/// A coded refusal whose text is not its `Code`'s.
pub(crate) trait Retext {
    fn retext(self, message: impl Into<String>) -> Self;
}

impl Retext for ControlError {
    fn retext(mut self, message: impl Into<String>) -> ControlError {
        self.message = message.into();
        self
    }
}
