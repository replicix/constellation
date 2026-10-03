//! `fs.*`: the filesystem registry over the control protocol (plan 31
//! §9.2, §9.8) — what the CLI's `fs create`/`fs list`/`fs passwd` do
//! in-process, as methods a daemon (and plan 37's CSI controller) can call.
//!
//! - **`fs.create` is idempotent by `(bucket, prefix)`.** `meta.json` at the
//!   prefix *is* the filesystem: when one exists, the call compares what it
//!   asked for (chunk size, compression, `e2e`, and the registered write
//!   mode when it names a registered filesystem) with what is there and
//!   answers the existing uuid (`created: false`) when they agree, or
//!   `Conflict`/`EEXIST` with the differences in `details` when they do
//!   not — never silently reusing or changing it. Two concurrent creators
//!   race on `meta.json`'s conditional PUT; the loser re-reads and takes the
//!   same comparison, so every caller gets the one uuid.
//! - **`fs.unlock` holds credentials in memory only**
//!   ([`CredentialSource::Static`] over an [`EphemeralSecretStore`]). The
//!   `fs.*` calls naming that filesystem sign with them (and read an E2E
//!   passphrase from them); when it names the filesystem this daemon's
//!   engine serves and that engine was started from a static source, the
//!   engine's own store is updated in place (a rotation).
//! - **Endpoint and region** come from the daemon's environment (the AWS
//!   chain, as every backend URL resolves); a call that asks for others is
//!   refused `Unsupported` rather than quietly using the environment's.

use super::EngineControl;
use crate::backend;
use crate::registry::{FsEntry, FsOverrides, Registry};
use constellation_control::proto::types::{
    FsCreateParams, FsCreated, FsDoctorCheck, FsDoctorReport, FsExportDocument, FsImportParams,
    FsInfo, FsListing, FsPasswdParams, FsUnlockParams,
};
use constellation_control::proto::ControlError;
use constellation_platform::{CredentialSource, EphemeralSecretStore, SecretStore};
use constellation_store_s3::{ChunkStore, FsMeta, StoreError};
use constellation_types::Code;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// The secret name an unlocked E2E passphrase is held under.
const E2E_PASSPHRASE: &str = "e2e_passphrase";

/// How long a `meta.json` read waits for a store that disagrees with itself.
const META_WAIT: Duration = Duration::from_secs(2);

fn failed(e: impl std::fmt::Display) -> ControlError {
    ControlError::failed(format!("{e:#}"))
}

fn store_err(e: StoreError) -> ControlError {
    match e {
        StoreError::NotFound => ControlError::not_found(e.to_string()),
        StoreError::AlreadyExists => ControlError::from(Code::Exists)
            .with_details(serde_json::json!({ "reason": e.to_string() })),
        other => failed(other),
    }
}

/// `(bucket, prefix)` of a backend URL, as `FsInfo` shows it.
fn split_url(url: &str) -> (String, String) {
    match url.strip_prefix("s3://") {
        Some(rest) => match rest.split_once('/') {
            Some((b, p)) => (b.to_string(), p.trim_matches('/').to_string()),
            None => (rest.to_string(), String::new()),
        },
        None => (url.to_string(), String::new()),
    }
}

/// The backend URL `fs.create` names: `s3://bucket/prefix`, or a local
/// directory (`file:///…` or an absolute path in `bucket`, for tests and
/// single-host use) with the prefix as a subdirectory.
fn url_of(bucket: &str, prefix: &str) -> Result<String, ControlError> {
    let prefix = prefix.trim_matches('/');
    if bucket.is_empty() {
        return Err(ControlError::invalid("bucket is empty"));
    }
    let local = bucket.starts_with('/') || bucket.starts_with("file://");
    Ok(match (local, prefix.is_empty()) {
        (true, true) => bucket.trim_end_matches('/').to_string(),
        (true, false) => format!("{}/{prefix}", bucket.trim_end_matches('/')),
        (false, true) => format!("s3://{bucket}"),
        (false, false) => format!("s3://{bucket}/{prefix}"),
    })
}

/// A self-contained registry document (`fs.export` → `fs.import`).
#[derive(Debug, Serialize, Deserialize)]
struct RegistryDocument {
    name: String,
    #[serde(flatten)]
    entry: FsEntry,
}

impl EngineControl {
    /// The credential source `fs.unlock` supplied for `fs`, if any.
    fn unlocked(&self, fs: &str) -> Option<Arc<CredentialSource>> {
        self.unlocked.lock().unwrap().get(fs).cloned()
    }

    /// A store at `url`, signed with what `fs.unlock` gave `key`, else
    /// with this engine's own credential source — the AWS chain for a CLI
    /// daemon, the unlocked keys of an engine pod started with
    /// `serve --await-unlock` (whose `fs.create` names its own location).
    async fn open_store(&self, url: &str, key: Option<&str>) -> Result<ChunkStore, ControlError> {
        let creds = key
            .and_then(|k| self.unlocked(k))
            .unwrap_or_else(|| self.engine.credentials().clone());
        let creds = Some(&creds);
        let (backend, _) = backend::open_backend_described_with(url, creds)
            .await
            .map_err(failed)?;
        Ok(ChunkStore::new(backend))
    }

    /// `name` → its registry entry, or `uuid` (or the backend URL itself,
    /// as an engine pod's CSI plugin names it before it knows the uuid) →
    /// this engine's backend.
    fn resolve_fs(&self, fs: &str) -> Result<(String, Option<FsEntry>), ControlError> {
        let registry = Registry::load().map_err(failed)?;
        if let Some(entry) = registry.entry(fs) {
            return Ok((entry.s3.clone(), Some(entry.clone())));
        }
        if fs == self.fs_uuid || fs == self.backend {
            return Ok((self.backend.clone(), None));
        }
        Err(ControlError::not_found(format!(
            "{fs:?} is neither a registered filesystem nor this daemon's"
        )))
    }

    pub(crate) async fn fs_list(&self) -> Result<FsListing, ControlError> {
        let registry = Registry::load().map_err(failed)?;
        let mut out = Vec::new();
        let mut own_listed = false;
        for (name, entry) in registry.iter() {
            let (bucket, prefix) = split_url(&entry.s3);
            let mut info = FsInfo {
                name: Some(name.clone()),
                bucket,
                prefix,
                write_mode: entry.write_mode.clone(),
                unlocked: self.unlocked(name).is_some(),
                ..FsInfo::default()
            };
            // A registered filesystem whose backend does not answer is
            // still listed (uuid empty): a listing must not fail because
            // one bucket is unreachable.
            let meta = if entry.s3 == self.backend {
                Some(self.engine.fsmeta().clone())
            } else {
                let load = async {
                    let store = self.open_store(&entry.s3, Some(name)).await.ok()?;
                    store.load_fs_waiting(META_WAIT).await.ok()
                };
                tokio::time::timeout(Duration::from_secs(10), load)
                    .await
                    .ok()
                    .flatten()
            };
            if let Some(meta) = meta {
                own_listed |= meta.uuid.to_string() == self.fs_uuid;
                info.uuid = meta.uuid.to_string();
                info.chunk_size = meta.chunk_size;
                info.compression = meta.compression.clone();
                info.e2e = meta.e2e;
            }
            out.push(info);
        }
        if !own_listed {
            let meta = self.engine.fsmeta();
            let (bucket, prefix) = split_url(&self.backend);
            out.push(FsInfo {
                uuid: meta.uuid.to_string(),
                name: None,
                bucket,
                prefix,
                chunk_size: meta.chunk_size,
                compression: meta.compression.clone(),
                e2e: meta.e2e,
                write_mode: self.write_mode.get().as_str().to_string(),
                unlocked: self.unlocked(&self.fs_uuid).is_some(),
                credentials_generation: self.engine.credentials().generation(),
                credentials_in_use: match &**self.engine.credentials() {
                    CredentialSource::Static(store) => store.in_use_generation(),
                    _ => 0,
                },
            });
        }
        Ok(FsListing { filesystems: out })
    }

    pub(crate) async fn fs_create(&self, p: FsCreateParams) -> Result<FsCreated, ControlError> {
        if p.endpoint.is_some() || p.region.is_some() {
            return Err(ControlError::unsupported(
                "fs.create resolves the endpoint and region from the daemon's environment",
            )
            .with_remediation("set AWS_ENDPOINT_URL / AWS_REGION for the daemon, or omit them"));
        }
        let url = url_of(&p.bucket, &p.prefix)?;
        let chunk_size = p
            .chunk_size
            .unwrap_or(constellation_fs_core::DEFAULT_CHUNK_SIZE);
        constellation_fs_core::validate_chunk_size(chunk_size)
            .map_err(|e| ControlError::invalid(e.to_string()))?;
        let compression: constellation_store_s3::CompressionSetting = p
            .compression
            .as_deref()
            .unwrap_or("zstd:3")
            .parse()
            .map_err(|e| ControlError::invalid(format!("compression: {e}")))?;
        if let Some(mode) = &p.write_mode {
            mode.parse::<crate::writeback::WriteMode>()
                .map_err(ControlError::invalid)?;
        }
        let key = p.name.as_deref();
        let store = self.open_store(&url, key).await?;
        let existing = match store.load_fs_waiting(META_WAIT).await {
            Ok(meta) => Some(meta),
            Err(StoreError::NotFound) => None,
            Err(e) => return Err(store_err(e)),
        };
        let (meta, created) = match existing {
            Some(meta) => (meta, false),
            None => {
                let mut meta = FsMeta::new(chunk_size, &compression.to_string());
                meta.e2e = p.e2e;
                if p.e2e {
                    meta.gossip_secret = None;
                    let source = key.and_then(|k| self.unlocked(k)).ok_or_else(|| {
                        ControlError::invalid("an e2e filesystem needs its passphrase")
                            .with_remediation("fs.unlock the name with e2e_passphrase first")
                    })?;
                    let passphrase = match &*source {
                        CredentialSource::Static(store) => store.get(E2E_PASSPHRASE).ok().flatten(),
                        _ => None,
                    }
                    .and_then(|s| s.expose_str().map(str::to_string))
                    .ok_or_else(|| ControlError::invalid("no e2e_passphrase was unlocked"))?;
                    meta.keyring = Some(
                        constellation_store_s3::create_keyring_block(&passphrase)
                            .map_err(failed)?,
                    );
                }
                match store.create_fs(&meta).await {
                    Ok(()) => (meta, true),
                    // Lost the race: whoever won made *a* filesystem here;
                    // compare against it like any existing one.
                    Err(StoreError::AlreadyExists) => (
                        store.load_fs_waiting(META_WAIT).await.map_err(store_err)?,
                        false,
                    ),
                    Err(e) => return Err(store_err(e)),
                }
            }
        };
        let registered =
            key.and_then(|name| Registry::load().ok().and_then(|r| r.entry(name).cloned()));
        if !created {
            let mut differs = serde_json::Map::new();
            if p.chunk_size.is_some_and(|c| c != meta.chunk_size) {
                differs.insert("chunk_size".into(), meta.chunk_size.into());
            }
            if p.compression.is_some() && compression.to_string() != meta.compression {
                differs.insert("compression".into(), meta.compression.clone().into());
            }
            if p.e2e != meta.e2e {
                differs.insert("e2e".into(), meta.e2e.into());
            }
            if let (Some(mode), Some(entry)) = (&p.write_mode, &registered) {
                if !entry.write_mode.is_empty() && &entry.write_mode != mode {
                    differs.insert("write_mode".into(), entry.write_mode.clone().into());
                }
            }
            if let Some(entry) = &registered {
                if entry.s3 != url {
                    differs.insert("name".into(), entry.s3.clone().into());
                }
            }
            if !differs.is_empty() {
                return Err(ControlError::from(Code::Exists)
                    .with_details(serde_json::json!({
                        "uuid": meta.uuid.to_string(),
                        "existing": differs,
                    }))
                    .with_remediation(
                        "a filesystem already exists at this (bucket, prefix) with other \
                         parameters; ask for those, or use another prefix",
                    ));
            }
        }
        if let Some(name) = key {
            Registry::load_locked()
                .map_err(failed)?
                .merge_and_save(
                    name,
                    FsOverrides {
                        s3: Some(url.clone()),
                        write_mode: p.write_mode.clone(),
                        ..Default::default()
                    },
                )
                .map_err(failed)?;
        }
        Ok(FsCreated {
            uuid: meta.uuid.to_string(),
            created,
        })
    }

    pub(crate) fn fs_export(&self, fs: &str) -> Result<FsExportDocument, ControlError> {
        let registry = Registry::load().map_err(failed)?;
        let entry = registry
            .entry(fs)
            .ok_or_else(|| ControlError::not_found(format!("{fs:?} is not registered")))?;
        let document = toml::to_string(&RegistryDocument {
            name: fs.to_string(),
            entry: entry.clone(),
        })
        .map_err(failed)?;
        Ok(FsExportDocument { document })
    }

    pub(crate) fn fs_import(
        &self,
        p: FsImportParams,
    ) -> Result<constellation_control::proto::types::FsInfo, ControlError> {
        let doc: RegistryDocument = toml::from_str(&p.document)
            .map_err(|e| ControlError::invalid(format!("the document: {e}")))?;
        let name = p.name.unwrap_or(doc.name);
        let e = doc.entry;
        let mut registry = Registry::load_locked().map_err(failed)?;
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
        let (bucket, prefix) = split_url(&entry.s3);
        Ok(FsInfo {
            name: Some(name.clone()),
            bucket,
            prefix,
            write_mode: entry.write_mode,
            unlocked: self.unlocked(&name).is_some(),
            ..FsInfo::default()
        })
    }

    pub(crate) async fn fs_passwd(&self, p: FsPasswdParams) -> Result<(), ControlError> {
        let (url, _) = self.resolve_fs(&p.fs)?;
        let store = self.open_store(&url, Some(&p.fs)).await?;
        let meta = store.load_fs_waiting(META_WAIT).await.map_err(store_err)?;
        if !meta.e2e {
            return Err(ControlError::invalid("filesystem is not in E2E mode"));
        }
        store
            .change_passphrase(p.old_passphrase.expose(), p.new_passphrase.expose())
            .await
            .map_err(|e| failed(format!("changing E2E passphrase: {e}")))
    }

    pub(crate) async fn fs_doctor(
        &self,
        fs: Option<String>,
    ) -> Result<FsDoctorReport, ControlError> {
        let targets: Vec<(String, String)> = match fs {
            Some(fs) => vec![(fs.clone(), self.resolve_fs(&fs)?.0)],
            None => {
                let registry = Registry::load().map_err(failed)?;
                let mut all: Vec<_> = registry
                    .iter()
                    .map(|(n, e)| (n.clone(), e.s3.clone()))
                    .collect();
                if !all.iter().any(|(_, url)| *url == self.backend) {
                    all.push((self.fs_uuid.clone(), self.backend.clone()));
                }
                all
            }
        };
        let mut checks = Vec::new();
        for (fs, url) in targets {
            let mut check = |name: &str, result: Result<String, String>| {
                let ok = result.is_ok();
                checks.push(FsDoctorCheck {
                    fs: fs.clone(),
                    check: name.to_string(),
                    ok,
                    detail: result.unwrap_or_else(|e| e),
                });
                ok
            };
            let store = match self.open_store(&url, Some(&fs)).await {
                Ok(store) => {
                    check("backend", Ok(url.clone()));
                    store
                }
                Err(e) => {
                    check("backend", Err(e.message));
                    continue;
                }
            };
            let meta = store.load_fs_waiting(META_WAIT).await;
            let meta_ok = check(
                "meta.json",
                meta.as_ref()
                    .map(|m| format!("uuid {}", m.uuid))
                    .map_err(|e| e.to_string()),
            );
            if !meta_ok {
                continue;
            }
            check(
                "conditional_writes",
                match store.probe_conditional_writes().await {
                    Ok(c) if c.create_if_absent && c.etag_cas => {
                        Ok("create-if-absent and etag CAS".into())
                    }
                    Ok(c) if c.create_if_absent => {
                        Ok("create-if-absent only (single writer)".into())
                    }
                    Ok(_) => Err("no conditional create".into()),
                    Err(e) => Err(e.to_string()),
                },
            );
        }
        Ok(FsDoctorReport { checks })
    }

    /// `fs.unlock` (module docs). A rotation of this engine's own static
    /// source is tried first: one authenticated read (`meta.json`) signed
    /// with the new pair, and a pair S3 refuses is refused here (`Denied`;
    /// `Unavailable` when S3 could not be asked) with the old one left in
    /// use — a typo in a rotated Secret must not stop every running engine
    /// at once.
    pub(crate) async fn fs_unlock(&self, p: FsUnlockParams) -> Result<String, ControlError> {
        let ticket = self
            .unlock_order
            .0
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let c = p.credentials;
        let has_keys = c.access_key_id.is_some() || c.secret_access_key.is_some();
        if has_keys && (c.access_key_id.is_none() || c.secret_access_key.is_none()) {
            return Err(ControlError::invalid(
                "access_key_id and secret_access_key go together",
            ));
        }
        if !has_keys && c.e2e_passphrase.is_none() {
            return Err(ControlError::invalid("no credentials to unlock"));
        }
        let (_, entry) = self.resolve_fs(&p.fs)?;
        let own = entry.as_ref().is_none_or(|e| e.s3 == self.backend);
        let rotation = own && matches!(&**self.engine.credentials(), CredentialSource::Static(_));
        if rotation {
            if let (Some(id), Some(secret)) = (&c.access_key_id, &c.secret_access_key) {
                let trial = EphemeralSecretStore::new();
                trial
                    .replace(&[
                        (
                            CredentialSource::ACCESS_KEY_ID,
                            Some(id.expose().as_bytes()),
                        ),
                        (
                            CredentialSource::SECRET_ACCESS_KEY,
                            Some(secret.expose().as_bytes()),
                        ),
                        (
                            CredentialSource::SESSION_TOKEN,
                            c.session_token.as_ref().map(|t| t.expose().as_bytes()),
                        ),
                    ])
                    .map_err(failed)?;
                self.probe_credentials(&p.fs, CredentialSource::Static(trial))
                    .await?;
            }
        }
        // Rotation of this engine's own static source, in place: one
        // generation, so the S3 provider never sees half a key pair and
        // re-resolves on its next request (`backend::SourceCredentials`).
        let store = match (own, &**self.engine.credentials()) {
            (true, CredentialSource::Static(store)) => store.clone(),
            _ => EphemeralSecretStore::new(),
        };
        let mut entries: Vec<(&str, Option<&[u8]>)> = Vec::new();
        if let (Some(id), Some(secret)) = (&c.access_key_id, &c.secret_access_key) {
            entries.push((
                CredentialSource::ACCESS_KEY_ID,
                Some(id.expose().as_bytes()),
            ));
            entries.push((
                CredentialSource::SECRET_ACCESS_KEY,
                Some(secret.expose().as_bytes()),
            ));
            entries.push((
                CredentialSource::SESSION_TOKEN,
                c.session_token.as_ref().map(|t| t.expose().as_bytes()),
            ));
            // A rotation replaces expiring keys with whatever it brings.
            entries.push((CredentialSource::EXPIRY, None));
        }
        if let Some(passphrase) = &c.e2e_passphrase {
            entries.push((E2E_PASSPHRASE, Some(passphrase.expose().as_bytes())));
        }
        // A slower trial read of an older push must not land after a newer
        // one: the last push (by arrival) wins.
        let mut applied = self.unlock_order.1.lock().unwrap();
        if ticket < *applied {
            return Ok(format!(
                "{}: superseded by a later fs.unlock; nothing changed",
                p.fs
            ));
        }
        *applied = ticket;
        store.replace(&entries).map_err(failed)?;
        if let (true, false, Some(passphrase)) = (own, rotation, &c.e2e_passphrase) {
            // An engine on its environment's keys: what a handoff hands its
            // successor is kept apart from `store` (a fresh one), and must
            // not go stale.
            if let Some(kept) = self.host.handoff_secrets() {
                kept.replace(&[(E2E_PASSPHRASE, Some(passphrase.expose().as_bytes()))])
                    .map_err(failed)?;
            }
        }
        let source = Arc::new(CredentialSource::Static(store));
        let mut unlocked = self.unlocked.lock().unwrap();
        unlocked.insert(p.fs.clone(), source.clone());
        if own {
            unlocked.insert(self.fs_uuid.clone(), source);
        }
        drop(unlocked);
        drop(applied);
        Ok(match rotation {
            true => format!("{}: credentials rotated in the running engine", p.fs),
            false => format!("{}: credentials held in memory for fs.* calls", p.fs),
        })
    }

    /// One authenticated read of this engine's `meta.json` signed by
    /// `source` (see [`Self::fs_unlock`]). The error says what S3 answered
    /// — its status and error code, never its body (the S3 client's
    /// connector withholds it) — and that nothing changed.
    async fn probe_credentials(
        &self,
        fs: &str,
        source: CredentialSource,
    ) -> Result<(), ControlError> {
        use constellation_control::proto::ErrorKind;
        let source = Arc::new(source);
        let checked = async {
            let (backend, _) = backend::open_backend_described_with(&self.backend, Some(&source))
                .await
                .map_err(|e| (ErrorKind::Denied, format!("{e:#}")))?;
            ChunkStore::new(backend)
                .load_fs()
                .await
                .map(drop)
                .map_err(
                    |e| match constellation_store_s3::classify(&e).is_transient() {
                        true => (
                            ErrorKind::Unavailable,
                            format!("S3 could not be asked (reading meta.json: {e})"),
                        ),
                        false => (
                            ErrorKind::Denied,
                            format!("reading meta.json with them: {e}"),
                        ),
                    },
                )
        };
        let (kind, why) = match tokio::time::timeout(Duration::from_secs(30), checked).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(refusal)) => refusal,
            Err(_) => (
                ErrorKind::Unavailable,
                "S3 did not answer the trial read within 30 s".to_string(),
            ),
        };
        Err(ControlError::new(
            kind,
            format!(
                "{fs}: the new credentials were refused, and the ones in use stay in use: {why}"
            ),
        ))
    }
}
