//! `fs.*`: the filesystem [`Manager`](crate::manager::Manager) over the
//! control protocol (plan 31 §9.2, §9.8) — the same calls the CLI's
//! `fs create`/`fs passwd`/`fs list`/`doctor` make in-process, as methods a
//! daemon (and plan 37's CSI controller) can call. This module maps the
//! protocol's parameters onto the manager and its answers back; the rules
//! are the manager's.
//!
//! - **`fs.create` is idempotent by `(bucket, prefix)`**
//!   ([`Existing::Compare`]): `meta.json` at the prefix *is* the
//!   filesystem, and a call that matches it answers its uuid (`created:
//!   false`); one that does not is `Conflict`/`EEXIST` with the differences
//!   in `details`. Two concurrent creators race on `meta.json`'s
//!   conditional PUT; the loser takes the same comparison.
//! - **`fs.unlock` holds credentials in memory only**
//!   ([`CredentialSource::Static`] over an [`EphemeralSecretStore`]). The
//!   `fs.*` calls naming that filesystem sign with them (and read an E2E
//!   passphrase from them); when it names the filesystem this daemon's
//!   engine serves and that engine was started from a static source, the
//!   engine's own store is updated in place (a rotation).
//! - **Endpoint and region** come from the daemon's environment (the AWS
//!   chain, as every backend URL resolves); a call that asks for others is
//!   refused `Unsupported` rather than quietly using the environment's.
//! - A `meta.json` read waits at most two seconds for a store that
//!   disagrees with itself (`Manager::with_meta_wait`): a control call
//!   answers.

use super::EngineControl;
use crate::backend;
use crate::e2e_pin::PinTarget;
use crate::manager::{
    credentials, CreateHooks, CreateRequest, DoctorReport, Existing, FsState, PasswdRequest,
};
use crate::registry::FsEntry;
use constellation_control::proto::types::{
    FsCreateParams, FsCreated, FsDoctorCheck, FsDoctorReport, FsExportDocument, FsImportParams,
    FsInfo, FsListing, FsPasswdParams, FsUnlockParams,
};
use constellation_control::proto::ControlError;
use constellation_platform::{CredentialSource, EphemeralSecretStore};
use constellation_store_s3::ChunkStore;
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;

/// How long a `meta.json` read waits for a store that disagrees with itself.
pub(crate) const META_WAIT: Duration = Duration::from_secs(2);

fn failed(e: impl std::fmt::Display) -> ControlError {
    ControlError::failed(format!("{e:#}"))
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

/// `fs.create`'s passphrase: what `fs.unlock` gave the name.
struct UnlockedPassphrase(Option<Arc<CredentialSource>>);

impl CreateHooks for UnlockedPassphrase {
    fn passphrase(&mut self) -> Result<Zeroizing<String>, ControlError> {
        let source = self.0.as_ref().ok_or_else(|| {
            ControlError::invalid("an e2e filesystem needs its passphrase")
                .with_remediation("fs.unlock the name with e2e_passphrase first")
        })?;
        credentials::source_passphrase(source)?
            .ok_or_else(|| ControlError::invalid("no e2e_passphrase was unlocked"))
    }
}

/// `fs.doctor`'s rows for one filesystem's report: `backend`,
/// `conditional_writes`, then — when those let the check go on —
/// `cas_semantics` and `versioning`, then `meta.json`.
fn doctor_checks(fs: &str, report: &DoctorReport, checks: &mut Vec<FsDoctorCheck>) {
    let mut check = |name: &str, result: Result<String, String>| {
        checks.push(FsDoctorCheck {
            fs: fs.to_string(),
            check: name.to_string(),
            ok: result.is_ok(),
            detail: result.unwrap_or_else(|e| e),
        });
    };
    check("backend", Ok(report.url.clone()));
    check(
        "conditional_writes",
        match &report.capabilities {
            Ok(c) if c.create_if_absent && c.etag_cas => Ok("create-if-absent and etag CAS".into()),
            Ok(c) if c.create_if_absent => Ok("create-if-absent only (single writer)".into()),
            Ok(_) => Err("no conditional create".into()),
            Err(e) => Err(e.clone()),
        },
    );
    if let Some(cas) = &report.cas {
        check(
            "cas_semantics",
            match cas {
                Ok(r) => {
                    let violations: Vec<&str> = r.violations().map(|p| p.name.as_str()).collect();
                    let unknown: Vec<&str> = r.unknown().map(|p| p.name.as_str()).collect();
                    if !violations.is_empty() {
                        Err(format!(
                            "not enforced atomically: {}; do not use it for more than one node",
                            violations.join(", ")
                        ))
                    } else if !unknown.is_empty() {
                        Ok(format!(
                            "{} probes; unknown semantics: {}",
                            r.probes.len(),
                            unknown.join(", ")
                        ))
                    } else {
                        Ok(format!("{} probes as expected", r.probes.len()))
                    }
                }
                Err(e) => Err(e.clone()),
            },
        );
        if let Ok(r) = cas {
            check("versioning", Ok(r.versioning.as_str().to_string()));
        }
    }
    if let Some(state) = &report.filesystem {
        check(
            "meta.json",
            match state {
                FsState::Present { uuid, .. } => Ok(format!("uuid {uuid}")),
                FsState::Missing(found) => Err(format!("no filesystem: {found}")),
                FsState::Unreadable(e) => Err(e.clone()),
            },
        );
    }
}

impl EngineControl {
    /// The credential source `fs.unlock` supplied for `fs`, if any.
    fn unlocked(&self, fs: &str) -> Option<Arc<CredentialSource>> {
        self.unlocked.lock().unwrap().get(fs).cloned()
    }

    /// What `fs.*` naming `key` signs with: the key pair `fs.unlock` gave
    /// it, else this engine's own credential source — the AWS chain for a
    /// CLI daemon, the unlocked keys of an engine pod started with
    /// `serve --await-unlock` (whose `fs.create` names its own location).
    /// An unlock of a passphrase alone signs with the latter
    /// (`credentials::signs`), as the gate's does.
    fn credentials_for(&self, key: Option<&str>) -> Arc<CredentialSource> {
        key.and_then(|k| self.unlocked(k))
            .filter(|source| credentials::signs(source))
            .unwrap_or_else(|| self.engine.credentials().clone())
    }

    /// `name` → its registry entry, or `uuid` (or the backend URL itself,
    /// as an engine pod's CSI plugin names it before it knows the uuid) →
    /// this engine's backend.
    fn resolve_fs(&self, fs: &str) -> Result<(String, Option<FsEntry>), ControlError> {
        if let Some(entry) = self.manager.entry(fs)? {
            return Ok((entry.s3.clone(), Some(entry)));
        }
        if fs == self.fs_uuid || fs == self.backend {
            return Ok((self.backend.clone(), None));
        }
        Err(ControlError::not_found(format!(
            "{fs:?} is neither a registered filesystem nor this daemon's"
        )))
    }

    pub(crate) async fn fs_list(&self) -> Result<FsListing, ControlError> {
        let mut out = Vec::new();
        let mut own_listed = false;
        for (name, entry) in self.manager.list()? {
            let (bucket, prefix) = split_url(&entry.s3);
            let mut info = FsInfo {
                name: Some(name.clone()),
                bucket,
                prefix,
                write_mode: entry.write_mode.clone(),
                unlocked: self.unlocked(&name).is_some(),
                ..FsInfo::default()
            };
            // A registered filesystem whose backend does not answer is
            // still listed (uuid empty): a listing must not fail because
            // one bucket is unreachable.
            let meta = if entry.s3 == self.backend {
                Some(self.engine.fsmeta().clone())
            } else {
                let load = async {
                    let creds = self.credentials_for(Some(&name));
                    let (store, _) = self.manager.open(&entry.s3, Some(&creds)).await.ok()?;
                    self.manager.load_meta(&store).await.ok()
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
        let key = p.name.as_deref();
        let req = CreateRequest {
            name: p.name.clone(),
            chunk_size: p.chunk_size,
            compression: p.compression.clone(),
            e2e: p.e2e,
            write_mode: p.write_mode.clone(),
            credentials: Some(self.credentials_for(key)),
            ..CreateRequest::new(url, Existing::Compare)
        };
        let mut hooks = UnlockedPassphrase(key.and_then(|k| self.unlocked(k)));
        let created = self.manager.create(req, &mut hooks).await?;
        Ok(FsCreated {
            uuid: created.meta.uuid.to_string(),
            created: created.created,
        })
    }

    pub(crate) fn fs_export(&self, fs: &str) -> Result<FsExportDocument, ControlError> {
        Ok(FsExportDocument {
            document: self.manager.export_document(fs)?,
        })
    }

    pub(crate) fn fs_import(&self, p: FsImportParams) -> Result<FsInfo, ControlError> {
        let (name, entry) = self.manager.import(&p.document, p.name)?;
        let (bucket, prefix) = split_url(&entry.s3);
        Ok(FsInfo {
            unlocked: self.unlocked(&name).is_some(),
            name: Some(name),
            bucket,
            prefix,
            write_mode: entry.write_mode,
            ..FsInfo::default()
        })
    }

    pub(crate) async fn fs_passwd(&self, p: FsPasswdParams) -> Result<(), ControlError> {
        let (url, entry) = self.resolve_fs(&p.fs)?;
        let pin = match entry {
            Some(_) => PinTarget::named(&p.fs, &url),
            None => PinTarget::unnamed(&url),
        };
        let req = PasswdRequest {
            credentials: Some(self.credentials_for(Some(&p.fs))),
            url,
            pin,
        };
        let mut secrets = || {
            Ok((
                Zeroizing::new(p.old_passphrase.expose().to_string()),
                Zeroizing::new(p.new_passphrase.expose().to_string()),
            ))
        };
        let outcome = self.manager.passwd(req, &mut secrets).await?;
        if let Some(warning) = outcome.warning {
            tracing::warn!(fs = %p.fs, "fs.passwd: {warning}");
        }
        Ok(())
    }

    pub(crate) async fn fs_doctor(
        &self,
        fs: Option<String>,
    ) -> Result<FsDoctorReport, ControlError> {
        let targets: Vec<(String, String)> = match fs {
            Some(fs) => vec![(fs.clone(), self.resolve_fs(&fs)?.0)],
            None => {
                let mut all: Vec<_> = self
                    .manager
                    .list()?
                    .into_iter()
                    .map(|(n, e)| (n, e.s3))
                    .collect();
                if !all.iter().any(|(_, url)| *url == self.backend) {
                    all.push((self.fs_uuid.clone(), self.backend.clone()));
                }
                all
            }
        };
        let mut checks = Vec::new();
        for (fs, url) in targets {
            let creds = self.credentials_for(Some(&fs));
            match self.manager.doctor(&url, Some(&creds)).await {
                Ok(report) => doctor_checks(&fs, &report, &mut checks),
                Err(e) => checks.push(FsDoctorCheck {
                    fs,
                    check: "backend".into(),
                    ok: false,
                    detail: e.message,
                }),
            }
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
        let c = &p.credentials;
        let has_keys = credentials::validate(c)?;
        let (_, entry) = self.resolve_fs(&p.fs)?;
        let own = entry.as_ref().is_none_or(|e| e.s3 == self.backend);
        let rotation = own && matches!(&**self.engine.credentials(), CredentialSource::Static(_));
        if rotation && has_keys {
            let trial = EphemeralSecretStore::new();
            trial
                .replace(&credentials::key_entries(c))
                .map_err(failed)?;
            self.probe_credentials(&p.fs, CredentialSource::Static(trial))
                .await?;
        }
        // Rotation of this engine's own static source, in place: one
        // generation, so the S3 provider never sees half a key pair and
        // re-resolves on its next request (`backend::SourceCredentials`).
        let store = match (own, &**self.engine.credentials()) {
            (true, CredentialSource::Static(store)) => store.clone(),
            _ => EphemeralSecretStore::new(),
        };
        let entries = credentials::entries(c);
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
                kept.replace(&[(
                    credentials::E2E_PASSPHRASE,
                    Some(passphrase.expose().as_bytes()),
                )])
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
