//! [`Manager::doctor`]: `doctor` and `fs.doctor`.

use super::Manager;
use constellation_control::proto::ControlError;
use constellation_platform::CredentialSource;
use constellation_store_s3::{Capabilities, CasProbeReport, MissingFs};
use std::sync::Arc;

/// What a filesystem's prefix holds.
#[derive(Debug, Clone)]
pub enum FsState {
    /// No `meta.json`; what the prefix holds instead.
    Missing(MissingFs),
    Present {
        uuid: String,
        format_version: u32,
    },
    /// `meta.json` exists but could not be read.
    Unreadable(String),
}

/// What [`Manager::doctor`] found, step by step. A step that ends the
/// check leaves the later ones `None`: a backend without create-if-absent
/// is unusable whatever else it does, and one that answers a conditional
/// write non-atomically must not be trusted with a filesystem.
#[derive(Debug, Clone)]
pub struct DoctorReport {
    /// Where the URL resolved to (endpoint, region, credentials).
    pub backend: String,
    /// The URL as given.
    pub url: String,
    /// Create-if-absent (`If-None-Match`) and etag CAS (`If-Match`).
    pub capabilities: Result<Capabilities, String>,
    /// What the provider answers at each CAS edge, and bucket versioning
    /// (plan 30 §M4 items 1 and 4).
    pub cas: Option<Result<CasProbeReport, String>>,
    pub filesystem: Option<FsState>,
}

impl DoctorReport {
    /// Whether the backend is usable at all (create-if-absent, no
    /// conditional write answered non-atomically).
    pub fn usable(&self) -> bool {
        matches!(&self.capabilities, Ok(c) if c.create_if_absent)
            && matches!(&self.cas, Some(Ok(r)) if r.violations().next().is_none())
    }
}

impl Manager {
    /// Verify the backend at `url`: conditional writes, what the provider
    /// answers at each CAS edge, and whether a filesystem lives at the
    /// prefix — asked with a `HEAD` first (module docs). Fails only when
    /// the backend cannot be opened.
    pub async fn doctor(
        &self,
        url: &str,
        credentials: Option<&Arc<CredentialSource>>,
    ) -> Result<DoctorReport, ControlError> {
        let (store, info) = self.open(url, credentials).await?;
        let mut report = DoctorReport {
            backend: info.to_string(),
            url: url.to_string(),
            capabilities: store
                .probe_conditional_writes()
                .await
                .map_err(|e| e.to_string()),
            cas: None,
            filesystem: None,
        };
        if !matches!(&report.capabilities, Ok(c) if c.create_if_absent) {
            return Ok(report);
        }
        let cas = constellation_store_s3::probe_cas_semantics(store.inner())
            .await
            .map_err(|e| e.to_string());
        let trusted = matches!(&cas, Ok(r) if r.violations().next().is_none());
        report.cas = Some(cas);
        if !trusted {
            return Ok(report);
        }
        report.filesystem = Some(match store.fs_exists().await {
            Ok(false) => FsState::Missing(store.locate_missing_fs().await),
            Ok(true) => match self.load_meta(&store).await {
                Ok(meta) => FsState::Present {
                    uuid: meta.uuid.to_string(),
                    format_version: meta.format_version,
                },
                Err(e) => FsState::Unreadable(e.to_string()),
            },
            Err(e) => FsState::Unreadable(e.to_string()),
        });
        Ok(report)
    }
}
