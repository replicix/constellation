//! `constellation doctor`'s conditional-write probes (plan 30 §M4 items 1
//! and 4): what this provider actually answers at each CAS edge, and
//! whether bucket versioning is on.
//!
//! [`crate::store::ChunkStore::probe_conditional_writes`] asks only
//! "does `If-None-Match`/`If-Match` work at all". The CAS rules in
//! [`crate::cas`] also depend on *which* status comes back when it does,
//! and on the create and swap being atomic under concurrency, which is
//! the one property every lease, segment and commit relies on and which a
//! sequential probe cannot see. So each probe records the observed answer
//! (status and `object_store` variant), what `crate::cas::classify` makes
//! of it, and whether that is a known meaning:
//!
//! | probe | known answers |
//! |---|---|
//! | create over an existing key | 412 (AWS, MinIO, R2), 304 (some older gateways) → lost race |
//! | `If-Match` with a stale etag | 412 → lost race |
//! | `If-Match` on a missing key | 404 (AWS) → missing, 412 (R2, MinIO) → lost race — both re-read |
//! | N concurrent creates of one key | exactly one success; the rest lost races or 409s |
//! | N concurrent swaps from one etag | exactly one success; the rest lost races or 409s |
//!
//! Anything else is reported as unknown semantics and `doctor` warns: the
//! CAS rules may then misjudge a race on this provider. Two winners in a
//! concurrent probe is worse than unknown — it means the provider does not
//! enforce the precondition atomically — and `doctor` fails.
//!
//! **Versioning** (item 4) is read from the probe PUT's `x-amz-version-id`
//! (`PutResult::version`), so it costs no extra request and needs no
//! bucket-level API: a version id means versioning is enabled, the literal
//! `null` means it is suspended, and no header means it is off (or the
//! provider does not report it). Nothing relies on versioning; with it on,
//! every GC delete leaves a noncurrent version behind, which costs storage
//! until a lifecycle rule expires it, so `doctor` says so.

use crate::cas::{classify, http_status, CasCode};
use crate::error::StoreError;
use futures::future::join_all;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// How many writers race in the concurrent probes.
const RACERS: usize = 8;

/// One probe's outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasProbe {
    /// What was tried, e.g. `"create over an existing key"`.
    pub name: String,
    /// What came back, e.g. `"412 Precondition → lost race"` or, for a
    /// concurrent probe, a tally.
    pub observed: String,
    /// The meaning `crate::cas` assigns, when the answer is a known one.
    pub known: bool,
    /// The probe found a correctness problem (not just an unknown code):
    /// a precondition that was not enforced.
    pub violation: bool,
}

/// Bucket versioning as seen on the probe PUT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Versioning {
    Enabled,
    Suspended,
    /// No version id on the PUT: off, or not reported by this provider.
    OffOrUnreported,
}

impl Versioning {
    pub fn as_str(self) -> &'static str {
        match self {
            Versioning::Enabled => "enabled",
            Versioning::Suspended => "suspended",
            Versioning::OffOrUnreported => "off (or not reported)",
        }
    }
}

/// Everything [`probe_cas_semantics`] learned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasProbeReport {
    pub probes: Vec<CasProbe>,
    pub versioning: Versioning,
}

impl CasProbeReport {
    /// Probes whose answer `crate::cas` has no known meaning for.
    pub fn unknown(&self) -> impl Iterator<Item = &CasProbe> {
        self.probes.iter().filter(|p| !p.known && !p.violation)
    }

    /// Probes that found a precondition not enforced.
    pub fn violations(&self) -> impl Iterator<Item = &CasProbe> {
        self.probes.iter().filter(|p| p.violation)
    }
}

/// `object_store`'s variant name, for the report.
fn variant(err: &object_store::Error) -> &'static str {
    match err {
        object_store::Error::AlreadyExists { .. } => "AlreadyExists",
        object_store::Error::Precondition { .. } => "Precondition",
        object_store::Error::NotModified { .. } => "NotModified",
        object_store::Error::NotFound { .. } => "NotFound",
        object_store::Error::NotImplemented { .. } => "NotImplemented",
        object_store::Error::PermissionDenied { .. } => "PermissionDenied",
        object_store::Error::Unauthenticated { .. } => "Unauthenticated",
        object_store::Error::Generic { .. } => "Generic",
        _ => "other",
    }
}

/// `"<status> <variant> → <meaning>"` for one error.
fn describe(err: &object_store::Error, mode: &PutMode) -> (String, Option<CasCode>) {
    let status = http_status(err)
        .map(|s| s.to_string())
        .unwrap_or_else(|| "no status".to_string());
    let code = classify(err, mode);
    let meaning = match code {
        Some(CasCode::Lost) => "lost race",
        Some(CasCode::Busy) => "busy, retry the attempt",
        Some(CasCode::Missing) => "missing, re-read",
        None => "not a CAS answer",
    };
    (format!("{status} {} → {meaning}", variant(err)), code)
}

async fn put(
    store: &dyn ObjectStore,
    key: &Path,
    body: &'static [u8],
    mode: PutMode,
) -> Result<object_store::PutResult, object_store::Error> {
    store
        .put_opts(key, PutPayload::from_static(body), PutOptions::from(mode))
        .await
}

/// Run the probes under `.doctor/<uuid>/` and clean up (best effort). An
/// error means the backend could not even be written to; a probe whose
/// answer is merely unexpected is reported, not returned as an error.
pub async fn probe_cas_semantics(
    store: &Arc<dyn ObjectStore>,
) -> Result<CasProbeReport, StoreError> {
    let base = format!(".doctor/{}", uuid::Uuid::new_v4());
    let key = |name: &str| Path::from(format!("{base}/{name}"));
    let store_ref = store.as_ref();
    let mut probes = Vec::new();

    // Create, then create again over it.
    let exists = key("exists");
    let first = put(store_ref, &exists, b"a", PutMode::Create).await?;
    let versioning = match first.version.as_deref() {
        Some("null") => Versioning::Suspended,
        Some(_) => Versioning::Enabled,
        None => Versioning::OffOrUnreported,
    };
    probes.push(match put(store_ref, &exists, b"b", PutMode::Create).await {
        Ok(_) => CasProbe {
            name: "create over an existing key".into(),
            observed: "success (If-None-Match ignored)".into(),
            known: false,
            violation: true,
        },
        Err(e) => {
            let (observed, code) = describe(&e, &PutMode::Create);
            CasProbe {
                name: "create over an existing key".into(),
                known: code == Some(CasCode::Lost),
                observed,
                violation: false,
            }
        }
    });

    // `If-Match` with the right etag, then with the now-stale one.
    let version = UpdateVersion {
        e_tag: first.e_tag.clone(),
        version: first.version.clone(),
    };
    match put(store_ref, &exists, b"c", PutMode::Update(version.clone())).await {
        Ok(_) => {
            let mode = PutMode::Update(version);
            probes.push(match put(store_ref, &exists, b"d", mode.clone()).await {
                Ok(_) => CasProbe {
                    name: "If-Match with a stale etag".into(),
                    observed: "success (If-Match ignored)".into(),
                    known: false,
                    violation: true,
                },
                Err(e) => {
                    let (observed, code) = describe(&e, &mode);
                    CasProbe {
                        name: "If-Match with a stale etag".into(),
                        known: code == Some(CasCode::Lost),
                        observed,
                        violation: false,
                    }
                }
            });
        }
        Err(e) => probes.push(CasProbe {
            name: "If-Match with the current etag".into(),
            observed: format!("{} ({e})", variant(&e)),
            known: false,
            violation: false,
        }),
    }

    // `If-Match` against a key that does not exist.
    let missing = key("missing");
    let mode = PutMode::Update(UpdateVersion {
        e_tag: Some("\"0123456789abcdef0123456789abcdef\"".into()),
        version: None,
    });
    probes.push(match put(store_ref, &missing, b"x", mode.clone()).await {
        Ok(_) => CasProbe {
            name: "If-Match on a missing key".into(),
            observed: "success (If-Match ignored)".into(),
            known: false,
            violation: true,
        },
        Err(e) => {
            let (observed, code) = describe(&e, &mode);
            CasProbe {
                name: "If-Match on a missing key".into(),
                known: matches!(code, Some(CasCode::Missing | CasCode::Lost)),
                observed,
                violation: false,
            }
        }
    });

    // Concurrent creates of one fresh key.
    let raced = key("raced");
    let bodies: [&'static [u8]; RACERS] = [b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7"];
    let results = join_all(
        bodies
            .iter()
            .map(|body| put(store_ref, &raced, body, PutMode::Create)),
    )
    .await;
    probes.push(tally(
        "concurrent creates of one key",
        &results,
        &PutMode::Create,
    ));

    // Concurrent swaps from one etag.
    let swapped = key("swapped");
    let base_put = put(store_ref, &swapped, b"base", PutMode::Overwrite).await?;
    let mode = PutMode::Update(UpdateVersion {
        e_tag: base_put.e_tag,
        version: base_put.version,
    });
    let results = join_all(
        bodies
            .iter()
            .map(|body| put(store_ref, &swapped, body, mode.clone())),
    )
    .await;
    probes.push(tally("concurrent swaps from one etag", &results, &mode));

    for name in ["exists", "missing", "raced", "swapped"] {
        let _ = store.delete(&key(name)).await;
    }
    Ok(CasProbeReport { probes, versioning })
}

/// Summarize a concurrent probe: exactly one success is required, every
/// other answer must be a lost race or a 409.
fn tally(
    name: &str,
    results: &[Result<object_store::PutResult, object_store::Error>],
    mode: &PutMode,
) -> CasProbe {
    let mut won = 0usize;
    let mut lost = 0usize;
    let mut busy = 0usize;
    let mut other: Vec<String> = Vec::new();
    for r in results {
        match r {
            Ok(_) => won += 1,
            Err(e) => match classify(e, mode) {
                Some(CasCode::Lost) => lost += 1,
                Some(CasCode::Busy) => busy += 1,
                _ => other.push(describe(e, mode).0),
            },
        }
    }
    let mut observed = format!("{won} won, {lost} lost (412), {busy} busy (409)");
    if !other.is_empty() {
        other.sort();
        other.dedup();
        observed.push_str(&format!(", other: {}", other.join("; ")));
    }
    CasProbe {
        name: name.into(),
        observed,
        known: won == 1 && other.is_empty(),
        violation: won > 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// The in-memory store has exactly the semantics the CAS rules expect:
    /// every probe is known, nothing is violated, versioning is not
    /// reported.
    #[tokio::test]
    async fn in_memory_semantics_are_all_known() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let report = probe_cas_semantics(&store).await.unwrap();
        assert_eq!(report.probes.len(), 5, "{report:#?}");
        assert!(report.unknown().next().is_none(), "{report:#?}");
        assert!(report.violations().next().is_none(), "{report:#?}");
        assert_eq!(report.versioning, Versioning::OffOrUnreported);
    }

    /// A store that answers a stale `If-Match` with a 500 is reported as
    /// unknown semantics (a warning), not as a violation.
    #[tokio::test]
    async fn an_unexpected_code_is_unknown_not_a_violation() {
        use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
        let faulty = FaultyStore::new();
        // Put #1 creates `exists`, #2 re-creates it, #3 swaps it, #4 is the
        // stale swap.
        faulty.script(OpKind::Put, "/exists", Calls::Nth(4), Fault::Status(500));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let report = probe_cas_semantics(&store).await.unwrap();
        let stale = report
            .probes
            .iter()
            .find(|p| p.name == "If-Match with a stale etag")
            .unwrap();
        assert!(!stale.known && !stale.violation, "{stale:?}");
        assert!(stale.observed.starts_with("500"), "{stale:?}");
        assert_eq!(report.unknown().count(), 1);
    }

    /// A provider that ignores `If-None-Match` is a violation.
    #[tokio::test]
    async fn two_winners_is_a_violation() {
        let results: Vec<Result<object_store::PutResult, object_store::Error>> = (0..2)
            .map(|_| {
                Ok(object_store::PutResult {
                    e_tag: None,
                    version: None,
                    extensions: Default::default(),
                })
            })
            .collect();
        let probe = tally("x", &results, &PutMode::Create);
        assert!(probe.violation && !probe.known);
    }
}
