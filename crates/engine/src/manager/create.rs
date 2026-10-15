//! [`Manager::create`]: `fs create`, `fs.create`, and `serve --create`.

use super::{failed, repoint_err, store_err, store_failed, Manager, Retext};
use crate::e2e_pin::{self, PinTarget};
use crate::registry::{self, FsEntry, FsOverrides, Registry};
use constellation_control::proto::ControlError;
use constellation_platform::CredentialSource;
use constellation_store_s3::{CompressionSetting, FsMeta, PreflightCheck, StoreError};
use constellation_types::Code;
use std::sync::Arc;
use zeroize::Zeroizing;

/// What a create does when a filesystem already exists at the URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Existing {
    /// Refuse (`Exists`): `fs create` makes a new filesystem, and says so
    /// when there already is one.
    Refuse,
    /// Answer the existing one when every parameter the caller gave
    /// matches it, and refuse (`Exists`, the differences in `details`)
    /// otherwise: `fs.create` is idempotent by `(bucket, prefix)` (plan 31
    /// §9.2), so a CSI controller calls it on every `CreateVolume`.
    Compare,
    /// Use the existing one as it is (`serve --create`: the controller's
    /// own `fs.create` then compares).
    Accept,
}

/// A create. `None` parameters take the defaults: the default chunk size,
/// `zstd:3`, `CONSTELLATION_ACK`'s acknowledgement policy (else none
/// recorded, `local`).
#[derive(Clone)]
pub struct CreateRequest {
    /// The backend URL (`s3://bucket/prefix`, or a local directory).
    pub url: String,
    /// Register the filesystem under this name.
    pub name: Option<String>,
    pub chunk_size: Option<u32>,
    pub compression: Option<String>,
    pub e2e: bool,
    /// The logical size cap (0 or `None`: unbounded).
    pub max_size: Option<u64>,
    pub ack_policy: Option<String>,
    /// Plan 30 §M10's `f` (0: every write-eligible node is a member).
    pub epoch_slack: u32,
    /// The name's node-level write mode, recorded in the registry.
    pub write_mode: Option<String>,
    pub existing: Existing,
    /// What to sign with (`None`: the environment's AWS chain).
    pub credentials: Option<Arc<CredentialSource>>,
}

impl CreateRequest {
    /// A create of `url` with every parameter at its default.
    pub fn new(url: impl Into<String>, existing: Existing) -> CreateRequest {
        CreateRequest {
            url: url.into(),
            name: None,
            chunk_size: None,
            compression: None,
            e2e: false,
            max_size: None,
            ack_policy: None,
            epoch_slack: 0,
            write_mode: None,
            existing,
            credentials: None,
        }
    }
}

/// What the caller supplies while a create runs.
pub trait CreateHooks {
    /// The backend preflight's outcome, before anything is decided on it
    /// (the CLI prints it). Called only when a filesystem is about to be
    /// created.
    fn preflight(&mut self, _checks: &[PreflightCheck]) {}

    /// The new E2E filesystem's passphrase. Called only when an E2E
    /// filesystem is about to be created, after the preflight passed and
    /// before anything is written: an abort here leaves nothing behind.
    fn passphrase(&mut self) -> Result<Zeroizing<String>, ControlError>;
}

/// What a create came to.
#[derive(Debug, Clone)]
pub struct Created {
    /// The filesystem at the URL: the new one, or the existing one an
    /// [`Existing::Compare`]/[`Existing::Accept`] create answered.
    pub meta: FsMeta,
    /// False when it already existed.
    pub created: bool,
    /// It did not exist when asked, and a concurrent creator won the race.
    pub raced: bool,
    /// The compression setting asked for, normalised.
    pub compression: CompressionSetting,
    /// The S3 endpoint the URL resolved to (`None`: a local directory).
    pub endpoint: Option<String>,
}

impl Manager {
    /// Create the filesystem `req` describes (module docs, "the rules it
    /// owns"), and register it when it names one.
    pub async fn create(
        &self,
        req: CreateRequest,
        hooks: &mut (dyn CreateHooks + Send),
    ) -> Result<Created, ControlError> {
        let chunk_size = req
            .chunk_size
            .unwrap_or(constellation_fs_core::DEFAULT_CHUNK_SIZE);
        constellation_fs_core::validate_chunk_size(chunk_size)
            .map_err(|e| ControlError::invalid(e.to_string()))?;
        let compression: CompressionSetting = req
            .compression
            .as_deref()
            .unwrap_or("zstd:3")
            .parse()
            .map_err(|e| ControlError::invalid(format!("compression: {e}")))?;
        if let Some(mode) = &req.write_mode {
            mode.parse::<crate::writeback::WriteMode>()
                .map_err(|e| ControlError::invalid(format!("write mode {mode:?}: {e}")))?;
        }
        let ack_policy = crate::authority_driver::ack_policy_flag(req.ack_policy.as_deref())
            .map_err(|e| ControlError::invalid(format!("{e:#}")))?;
        if req.epoch_slack > 0 {
            let ttl = crate::lease::lease_ttl_ms();
            constellation_store_s3::PromiseConfig::from_env(ttl)
                .validate(ttl)
                .map_err(|why| {
                    ControlError::invalid(format!("--epoch-slack {}: {why}", req.epoch_slack))
                })?;
        }
        if let Some(name) = &req.name {
            registry::validate_name(name).map_err(|e| ControlError::invalid(format!("{e:#}")))?;
            // Fail fast, before the preflight and the prompt; checked again
            // under the registry's lock, which the PUT is made under.
            self.registry()?
                .check_repoint(name, &req.url)
                .map_err(repoint_err)?;
        }

        let (store, info) = self.open(&req.url, req.credentials.as_ref()).await?;
        let endpoint = info.endpoint().map(str::to_string);
        let exists = store.fs_exists().await.map_err(store_err)?;
        let refuse = || {
            ControlError::from(Code::Exists)
                .with_details(serde_json::json!({ "url": req.url }))
                .retext(format!(
                    "creating filesystem: {}",
                    StoreError::AlreadyExists
                ))
                .with_remediation(
                    "`constellation mount NAME MOUNTPOINT --s3 URL` registers an existing \
                     filesystem",
                )
        };
        // A named create holds the registry's lock from right before the
        // conditional PUT (the name's URL checked again under it) until the
        // name is registered: nothing repoints the name in between, and a
        // filesystem made here cannot be left unregistered by a check that
        // fails after it. Not across the preflight or the passphrase prompt,
        // which can take as long as a person does.
        let lock_name = || -> Result<Option<Registry>, ControlError> {
            let Some(name) = &req.name else {
                return Ok(None);
            };
            let reg = self.registry_locked()?;
            reg.check_repoint(name, &req.url).map_err(repoint_err)?;
            Ok(Some(reg))
        };
        let (meta, created, raced, held) = if exists {
            if req.existing == Existing::Refuse {
                return Err(refuse());
            }
            let meta = self.load_meta(&store).await.map_err(store_err)?;
            (meta, false, false, lock_name()?)
        } else {
            // Every operation a filesystem needs, before anything is
            // written or asked for: an unusable backend fails with a
            // report, not a raw protocol error mid-create.
            let checks = store.preflight().await;
            hooks.preflight(&checks);
            let failures: Vec<_> = checks
                .iter()
                .filter(|c| c.required)
                .filter_map(|c| c.outcome.as_ref().err().map(|why| (c.name, why.clone())))
                .collect();
            if !failures.is_empty() {
                let list = failures
                    .iter()
                    .map(|(name, why)| format!("  - {name}: {why}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                return Err(ControlError::unsupported(format!(
                    "backend at {} is not usable as a constellation filesystem; the following \
                     required operations failed:\n{list}",
                    req.url
                ))
                .with_details(serde_json::json!({
                    "failed": failures
                        .iter()
                        .map(|(name, why)| serde_json::json!({ "operation": name, "reason": why }))
                        .collect::<Vec<_>>(),
                })));
            }
            let mut meta = FsMeta::new(chunk_size, &compression.to_string());
            meta.e2e = req.e2e;
            meta.max_logical_bytes = req.max_size.filter(|&n| n > 0);
            meta.ack_policy = ack_policy;
            meta.epoch_slack = (req.epoch_slack > 0).then_some(req.epoch_slack);
            if req.e2e {
                // The gossip topic of an E2E filesystem is seeded from the
                // passphrase-protected keyring: in plaintext `meta.json` it
                // would let a bucket reader join the topic. (Plaintext
                // filesystems keep it there: S3 is their trust boundary.)
                meta.gossip_secret = None;
                // The keyring goes into the one conditional PUT that makes
                // the filesystem: identity and keys, no orphan window.
                let passphrase = hooks.passphrase()?;
                if passphrase.is_empty() {
                    return Err(ControlError::invalid("passphrase must not be empty"));
                }
                meta.keyring = Some(
                    constellation_store_s3::create_keyring_block(&passphrase)
                        .map_err(|e| failed(format!("creating E2E keyring: {e}")))?,
                );
            }
            let held = lock_name()?;
            match store.create_fs(&meta).await {
                Ok(()) => (meta, true, false, held),
                Err(StoreError::AlreadyExists) if req.existing == Existing::Refuse => {
                    return Err(refuse());
                }
                // Lost the race: whoever won made *a* filesystem here;
                // take it like any existing one.
                Err(StoreError::AlreadyExists) => (
                    self.load_meta(&store).await.map_err(store_err)?,
                    false,
                    true,
                    held,
                ),
                Err(e) => return Err(store_failed(e, Some("creating filesystem"))),
            }
        };

        if !created && req.existing == Existing::Compare {
            let registered = match (&held, &req.name) {
                (Some(reg), Some(name)) => reg.entry(name),
                _ => None,
            };
            self.compare(&req, &compression, &meta, registered)?;
        }
        if let (Some(name), Some(mut reg)) = (&req.name, held) {
            reg.merge_and_save(
                name,
                FsOverrides {
                    s3: Some(req.url.clone()),
                    write_mode: req.write_mode.clone(),
                    // The endpoint is the URL's: a local one clears what a
                    // name repointed from S3 remembered.
                    endpoint: Some(endpoint.clone().unwrap_or_default()),
                    ..Default::default()
                },
            )
            .map_err(|e| failed(format!("registering the new filesystem: {e:#}")))?;
            drop(reg);
            if created {
                // What this host trusts about it from now on (`e2e_pin`);
                // the first unlock adds the master key's fingerprint.
                e2e_pin::record_with(self.pins(), &PinTarget::named(name, &req.url), &meta, None);
            }
        }
        Ok(Created {
            meta,
            created,
            raced,
            compression,
            endpoint,
        })
    }

    /// [`Existing::Compare`]: the parameters the caller gave against the
    /// filesystem that exists (and the write mode a registered name has).
    fn compare(
        &self,
        req: &CreateRequest,
        compression: &CompressionSetting,
        meta: &FsMeta,
        registered: Option<&FsEntry>,
    ) -> Result<(), ControlError> {
        let mut differs = serde_json::Map::new();
        if req.chunk_size.is_some_and(|c| c != meta.chunk_size) {
            differs.insert("chunk_size".into(), meta.chunk_size.into());
        }
        if req.compression.is_some() && compression.to_string() != meta.compression {
            differs.insert("compression".into(), meta.compression.clone().into());
        }
        if req.e2e != meta.e2e {
            differs.insert("e2e".into(), meta.e2e.into());
        }
        if req
            .max_size
            .is_some_and(|m| (m > 0).then_some(m) != meta.max_logical_bytes)
        {
            differs.insert("max_size".into(), meta.max_logical_bytes.into());
        }
        if req.ack_policy.is_some() {
            let asked = crate::authority_driver::ack_policy_flag(req.ack_policy.as_deref())
                .ok()
                .flatten();
            if asked != meta.ack_policy {
                differs.insert("ack_policy".into(), meta.ack_policy.clone().into());
            }
        }
        if req.epoch_slack > 0 && Some(req.epoch_slack) != meta.epoch_slack {
            differs.insert("epoch_slack".into(), meta.epoch_slack.into());
        }
        if let (Some(mode), Some(entry)) = (&req.write_mode, registered) {
            if !entry.write_mode.is_empty() && &entry.write_mode != mode {
                differs.insert("write_mode".into(), entry.write_mode.clone().into());
            }
        }
        if differs.is_empty() {
            return Ok(());
        }
        Err(ControlError::from(Code::Exists)
            .with_details(serde_json::json!({
                "uuid": meta.uuid.to_string(),
                "existing": differs,
            }))
            .with_remediation(
                "a filesystem already exists at this (bucket, prefix) with other parameters; ask \
                 for those, or use another prefix",
            ))
    }
}
