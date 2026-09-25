//! Conditional PUTs and what their error codes mean (plan 30 §M4 item 1).
//!
//! Every CAS site in this crate — lease create/swap, log segment create
//! (including a takeover's epoch marker), commit create, designations, the
//! node registry and the condemned-chunk pointer — commits with one
//! conditional PUT and must tell three failures apart:
//!
//! | code | on `If-None-Match: *` | on `If-Match: <etag>` | what it means | what we do |
//! |---|---|---|---|---|
//! | 412 | the key exists | the etag moved | a lost race | re-read, report the loss |
//! | 409 | another conditional write on the key is in flight | same | this attempt did **not** take effect | retry the *same* attempt |
//! | 404 | — | the object is gone | the version we matched no longer exists | re-read (it is a loss too) |
//!
//! plus the case none of the codes cover: **our own write landed, and the
//! reply we saw is not its reply.** `object_store` retries a conditional
//! PUT that got a 5xx (it retries every 5xx, 429 and 408 regardless of
//! idempotency); if the first attempt was applied and only its response
//! was lost, the retry sees the object we just wrote and gets a 412. The
//! same happens across a timeout, which `object_store` does *not* retry
//! for a conditional PUT (it is not idempotent) — our own next attempt
//! then meets our own earlier write. So a 412 (or a 404 on `If-Match`) is
//! only a lost race after checking that the object is not byte-for-byte
//! the one we tried to write ([`Verify::Body`]). Every body written at a
//! CAS site is unique to its writer and attempt (a lease carries its
//! holder and expiry, a registry claim a random nonce, a segment its
//! node id and records, a commit its author), so byte equality is proof
//! of authorship, not coincidence.
//!
//! ### How `object_store` 0.14 maps the codes (the limitation)
//!
//! Its generic HTTP layer maps 404 → `NotFound`, 304 → `NotModified`,
//! 412 → `Precondition` and **409 → `AlreadyExists`**. Its S3 client then
//! rewrites them per mode:
//!
//! - `PutMode::Create` (`If-None-Match: *`): a 412 or 304 becomes
//!   `AlreadyExists` wrapping the original `Precondition`/`NotModified`; a
//!   409 is **also** `AlreadyExists`, wrapping the raw HTTP error, and is
//!   not retried. The variant alone therefore cannot tell "the key exists"
//!   from "retry me". [`classify`] looks one level down: a wrapped
//!   `object_store::Error` is a 412/304, anything else is inspected for its
//!   status line ([`http_status`]).
//! - `PutMode::Update` (`If-Match`): 409 is retried inside `object_store`
//!   (`retry_on_conflict`), so an `AlreadyExists` that reaches us is a 409
//!   that outlasted those retries. A 404 is rewritten to `Precondition`
//!   ("for consistency with R2"), so 404 and 412 share a variant; the
//!   status line in the wrapped error still says which, and both lead to a
//!   re-read anyway, so a provider that words it differently costs nothing
//!   but the distinction in a log line.
//!
//! The raw HTTP error type (`RetryError`) is crate-private in
//! `object_store`, so the status is read from its `Display` text
//! (`"… status code: 409 Conflict …"`). That is a documented limitation:
//! a future `object_store` that rewords it degrades a 409 on create to a
//! lost race, which [`Verify::Body`]'s read-back then turns into a retry
//! (the object is absent), so the fallback is still correct, one GET
//! slower. `constellation doctor` records what each provider actually
//! returns ([`crate::probe::probe_cas_semantics`]).

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::time::Duration;

/// What a conditional PUT's failure means (see the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasCode {
    /// 412 (or 304 on `If-None-Match`): the precondition is false.
    Lost,
    /// 409: another conditional write on the key was in flight; this
    /// attempt did not take effect and may be retried as-is.
    Busy,
    /// 404 on `If-Match`: the object the attempt expected is gone.
    Missing,
}

/// The HTTP status an `object_store` error carries, when its text says.
///
/// Walks the error's source chain and looks for the generic HTTP layer's
/// `"status code: NNN"` phrasing (see the module doc for why the text).
pub fn http_status(err: &(dyn std::error::Error + 'static)) -> Option<u16> {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        let text = e.to_string();
        if let Some(code) = status_in(&text) {
            return Some(code);
        }
        cur = e.source();
    }
    None
}

fn status_in(text: &str) -> Option<u16> {
    let at = text.find("status code: ")? + "status code: ".len();
    let digits: String = text[at..].chars().take(3).collect();
    if digits.len() == 3 && digits.chars().all(|c| c.is_ascii_digit()) {
        digits.parse().ok()
    } else {
        None
    }
}

fn wraps_object_store_error(source: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    source.downcast_ref::<object_store::Error>().is_some()
}

fn says_not_found(source: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    if http_status(source) == Some(404) {
        return true;
    }
    let text = source.to_string().to_ascii_lowercase();
    text.contains("nosuchkey") || text.contains("not found")
}

/// Classify a conditional PUT's error (see the module doc's table). `None`
/// for anything that is not a CAS answer: a transport error, a 5xx that
/// outlasted `object_store`'s retries, a permission error. Those are
/// transient or fatal, never a race, and callers propagate them.
pub fn classify(err: &object_store::Error, mode: &PutMode) -> Option<CasCode> {
    match (err, mode) {
        (object_store::Error::AlreadyExists { source, .. }, PutMode::Create) => {
            if wraps_object_store_error(source.as_ref()) {
                // The S3 client's rewrite of a 412/304.
                Some(CasCode::Lost)
            } else if http_status(source.as_ref()) == Some(409) {
                Some(CasCode::Busy)
            } else {
                // A store without the HTTP layer (in-memory, local file
                // system) reporting a plain "exists".
                Some(CasCode::Lost)
            }
        }
        // Only a 409 that outlasted `object_store`'s own retries.
        (object_store::Error::AlreadyExists { .. }, _) => Some(CasCode::Busy),
        (object_store::Error::Precondition { source, .. }, PutMode::Update(_)) => {
            if says_not_found(source.as_ref()) {
                Some(CasCode::Missing)
            } else {
                Some(CasCode::Lost)
            }
        }
        (object_store::Error::Precondition { .. }, _) => Some(CasCode::Lost),
        (object_store::Error::NotModified { .. }, _) => Some(CasCode::Lost),
        (object_store::Error::NotFound { .. }, PutMode::Update(_)) => Some(CasCode::Missing),
        _ => None,
    }
}

/// Whether [`put_conditional`] reads the object back after a 412/404 to
/// see whether the write that "lost" was in fact our own (see the module
/// doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verify {
    /// Read the object back and compare it with the body we wrote.
    Body,
    /// The caller does its own ownership check (the log shipper tails the
    /// segment it collided with and recognizes its own node id and
    /// records), so a read-back here would only duplicate a GET.
    Caller,
}

/// The outcome of a conditional PUT that the store answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasPut {
    /// The object now holds our body: this attempt, a retry of it, or an
    /// earlier attempt whose reply was lost ([`Verify::Body`]).
    Won(UpdateVersion),
    /// 412: somebody else's write is there.
    Lost,
    /// 404 on `If-Match`: the object we expected is gone.
    Missing,
}

/// Retries of one attempt that the store answered 409 (default 5,
/// `CONSTELLATION_CAS_BUSY_RETRIES`), and the first backoff (doubling,
/// capped at 1 s).
pub fn busy_retries() -> u32 {
    std::env::var("CONSTELLATION_CAS_BUSY_RETRIES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(5)
}

const BUSY_BACKOFF: Duration = Duration::from_millis(50);
const BUSY_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// One conditional PUT with the module doc's rules applied:
///
/// - success → [`CasPut::Won`];
/// - 409 → the same attempt again (same body, same precondition), with
///   backoff, up to [`busy_retries`] times; a 409 that outlasts them is
///   returned as the store's error (transient: the caller retries later);
/// - 412 → [`CasPut::Lost`], or [`CasPut::Won`] if [`Verify::Body`] finds
///   our own body there; a create whose object turns out to be absent is
///   retried like a 409 (a 409 the classifier could not see);
/// - 404 on `If-Match` → [`CasPut::Missing`] (or `Won`, as above);
/// - anything else → the store's error, unchanged.
pub async fn put_conditional(
    store: &dyn ObjectStore,
    path: &Path,
    body: Bytes,
    mode: PutMode,
    verify: Verify,
) -> Result<CasPut, object_store::Error> {
    put_conditional_with(store, path, body, mode, verify, busy_retries()).await
}

/// [`put_conditional`] with an explicit 409 retry budget.
pub async fn put_conditional_with(
    store: &dyn ObjectStore,
    path: &Path,
    body: Bytes,
    mode: PutMode,
    verify: Verify,
    retries: u32,
) -> Result<CasPut, object_store::Error> {
    let mut backoff = BUSY_BACKOFF;
    let mut attempt = 0u32;
    loop {
        let result = store
            .put_opts(
                path,
                PutPayload::from(body.clone()),
                PutOptions::from(mode.clone()),
            )
            .await;
        let err = match result {
            Ok(r) => {
                return Ok(CasPut::Won(UpdateVersion {
                    e_tag: r.e_tag,
                    version: r.version,
                }))
            }
            Err(err) => err,
        };
        match classify(&err, &mode) {
            None => return Err(err),
            Some(CasCode::Busy) => {}
            Some(code) => {
                let lost = if code == CasCode::Lost {
                    CasPut::Lost
                } else {
                    CasPut::Missing
                };
                if verify == Verify::Caller {
                    return Ok(lost);
                }
                match read_back(store, path, &body).await {
                    Ok((meta, current)) => {
                        if current == body {
                            tracing::info!(
                                %path,
                                "conditional PUT answered {code:?}, but the object is our own \
                                 write (a retried or timed-out attempt had landed)"
                            );
                            return Ok(CasPut::Won(UpdateVersion {
                                e_tag: meta.e_tag,
                                version: meta.version,
                            }));
                        }
                        return Ok(lost);
                    }
                    // "It exists" with nothing there: a 409 whose wording
                    // the classifier did not recognize, or a delete in
                    // between. Either way the key is free: retry the create.
                    Err(object_store::Error::NotFound { .. }) if mode == PutMode::Create => {}
                    Err(object_store::Error::NotFound { .. }) => return Ok(CasPut::Missing),
                    Err(e) => return Err(e),
                }
            }
        }
        if attempt >= retries {
            tracing::warn!(
                %path,
                attempts = attempt + 1,
                "conditional PUT still conflicting (409); giving up for now"
            );
            return Err(err);
        }
        attempt += 1;
        tracing::debug!(%path, attempt, "conditional PUT answered 409; retrying the same attempt");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BUSY_BACKOFF_MAX);
    }
}

/// [`put_conditional`]'s read-back. A GET that returns a strict prefix
/// of `ours` may be a torn read of our own landed write (an emulator's
/// GET is not atomic against a concurrent PUT of the key; real S3's is):
/// taken at face value it would report our win as a lost race. So such a
/// body is re-read a few times, promptly (`crate::control`'s budget)
/// before the last one is returned. Any other body — a complete object,
/// or a torn one that already differs from ours — decides at once:
/// whatever it becomes, it is not our write.
async fn read_back(
    store: &dyn ObjectStore,
    path: &Path,
    ours: &Bytes,
) -> Result<(object_store::ObjectMeta, Bytes), object_store::Error> {
    let mut backoff = crate::control::TORN_READ_BACKOFF;
    let mut attempt = 1u32;
    loop {
        let res = store.get(path).await?;
        let meta = res.meta.clone();
        let current = res.bytes().await?;
        let maybe_torn = current.len() < ours.len() && ours.starts_with(&current);
        if !maybe_torn || attempt >= crate::control::TORN_READ_ATTEMPTS {
            return Ok((meta, current));
        }
        tracing::debug!(
            %path,
            attempt,
            bytes = current.len(),
            "CAS read-back is a prefix of our body (a torn read?); re-reading"
        );
        attempt += 1;
        tokio::time::sleep(backoff).await;
        backoff *= 2;
    }
}

/// A create-if-absent of a **content-addressed** object (a chunk, a blob,
/// a pack body or index): the key names its content, so an object already
/// there *is* these bytes and counts as success. Returns whether it
/// already existed.
///
/// This is where a 409 is most dangerous: `object_store` reports it as
/// `AlreadyExists`, which these sites used to read as "already durable"
/// while nothing had been written — a manifest naming the chunk would then
/// commit against an object S3 never stored. Here a 409 retries the create
/// ([`put_conditional`]); only a real "exists" is a dedup hit.
pub async fn create_content_addressed(
    store: &dyn ObjectStore,
    path: &Path,
    body: Bytes,
) -> Result<bool, object_store::Error> {
    match put_conditional(store, path, body, PutMode::Create, Verify::Caller).await? {
        CasPut::Won(_) => Ok(false),
        CasPut::Lost | CasPut::Missing => Ok(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
    use std::sync::Arc;

    fn p(s: &str) -> Path {
        Path::from(s)
    }

    #[test]
    fn status_is_read_from_the_http_error_text() {
        assert_eq!(
            status_in("Server returned non-2xx status code: 409 Conflict: busy"),
            Some(409)
        );
        assert_eq!(status_in("status code: 41"), None);
        assert_eq!(status_in("no status here"), None);
    }

    #[test]
    fn classification_matches_the_s3_clients_rewrites() {
        let create = PutMode::Create;
        let update = PutMode::Update(UpdateVersion {
            e_tag: Some("\"1\"".into()),
            version: None,
        });
        for (code, mode, want) in [
            (412, &create, CasCode::Lost),
            (304, &create, CasCode::Lost),
            (409, &create, CasCode::Busy),
            (412, &update, CasCode::Lost),
            (404, &update, CasCode::Missing),
            (409, &update, CasCode::Busy),
        ] {
            let err = crate::faulty::s3_error("k", code, mode);
            assert_eq!(classify(&err, mode), Some(want), "{code} on {mode:?}");
        }
        let five = crate::faulty::s3_error("k", 500, &create);
        assert_eq!(classify(&five, &create), None);
    }

    #[tokio::test]
    async fn a_409_retries_the_same_attempt() {
        let store = FaultyStore::new();
        store.script(OpKind::Put, "k", Calls::First(2), Fault::Status(409));
        let out = put_conditional(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"mine"),
            PutMode::Create,
            Verify::Body,
        )
        .await
        .unwrap();
        assert!(matches!(out, CasPut::Won(_)));
        assert_eq!(store.calls(OpKind::Put, "k"), 3);
    }

    #[tokio::test]
    async fn a_409_that_never_clears_is_the_stores_error() {
        let store = FaultyStore::new();
        store.script(OpKind::Put, "k", Calls::Every, Fault::Status(409));
        let out = put_conditional_with(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"mine"),
            PutMode::Create,
            Verify::Body,
            2,
        )
        .await;
        assert!(out.is_err());
        assert_eq!(store.calls(OpKind::Put, "k"), 3);
    }

    #[tokio::test]
    async fn our_own_landed_write_is_a_win_not_a_lost_race() {
        let store = FaultyStore::new();
        store.script(OpKind::Put, "k", Calls::Nth(1), Fault::AppliedThen(412));
        let out = put_conditional(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"mine"),
            PutMode::Create,
            Verify::Body,
        )
        .await
        .unwrap();
        assert!(matches!(out, CasPut::Won(_)));
    }

    /// A torn read-back (an emulator's GET racing a PUT of the key) must
    /// not turn our own landed write into a lost race.
    #[tokio::test]
    async fn a_torn_read_back_of_our_own_write_is_still_a_win() {
        let store = FaultyStore::new();
        store.script(OpKind::Put, "k", Calls::Nth(1), Fault::AppliedThen(412));
        store.script(OpKind::Get, "k", Calls::Nth(1), Fault::Truncated(5));
        let out = put_conditional(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"{\"holder\":7,\"epoch\":3}"),
            PutMode::Create,
            Verify::Body,
        )
        .await
        .unwrap();
        assert!(matches!(out, CasPut::Won(_)), "{out:?}");
        assert_eq!(
            store.calls(OpKind::Get, "k"),
            2,
            "the torn body was re-read"
        );
    }

    /// A read-back that already differs from our body decides at once.
    #[tokio::test]
    async fn a_torn_foreign_object_is_a_lost_race_without_re_reads() {
        let store = FaultyStore::new();
        store
            .inner()
            .put(
                &p("k"),
                PutPayload::from_static(b"{\"holder\":8,\"epoch\":4}"),
            )
            .await
            .unwrap();
        store.script(OpKind::Get, "k", Calls::Every, Fault::Truncated(12));
        let out = put_conditional(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"{\"holder\":7,\"epoch\":3}"),
            PutMode::Create,
            Verify::Body,
        )
        .await
        .unwrap();
        assert_eq!(out, CasPut::Lost);
        assert_eq!(store.calls(OpKind::Get, "k"), 1);
    }

    #[tokio::test]
    async fn a_foreign_object_is_a_lost_race() {
        let store = FaultyStore::new();
        store
            .inner()
            .put(&p("k"), PutPayload::from_static(b"theirs"))
            .await
            .unwrap();
        let out = put_conditional(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"mine"),
            PutMode::Create,
            Verify::Body,
        )
        .await
        .unwrap();
        assert_eq!(out, CasPut::Lost);
    }

    #[tokio::test]
    async fn a_404_on_if_match_is_missing() {
        let store = FaultyStore::new();
        let stale = UpdateVersion {
            e_tag: Some("\"nope\"".into()),
            version: None,
        };
        let out = put_conditional(
            store.as_ref(),
            &p("gone"),
            Bytes::from_static(b"mine"),
            PutMode::Update(stale),
            Verify::Body,
        )
        .await
        .unwrap();
        assert_eq!(out, CasPut::Missing);
    }

    /// A 409 on a content-addressed create is not a dedup hit: the object
    /// is created, and only a real "exists" reports `existed`.
    #[tokio::test]
    async fn a_409_on_a_content_addressed_create_is_not_a_hit() {
        let store = FaultyStore::new();
        store.script(OpKind::Put, "c", Calls::Nth(1), Fault::Status(409));
        let existed = create_content_addressed(store.as_ref(), &p("c"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert!(!existed);
        assert_eq!(
            store
                .inner()
                .get(&p("c"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"x")
        );
        let existed = create_content_addressed(store.as_ref(), &p("c"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert!(existed);
    }

    #[tokio::test]
    async fn a_500_and_a_timeout_are_errors_not_races() {
        let store: Arc<FaultyStore> = FaultyStore::new();
        store.script(OpKind::Put, "a", Calls::Every, Fault::Status(500));
        store.script(OpKind::Put, "b", Calls::Every, Fault::Timeout);
        for key in ["a", "b"] {
            let out = put_conditional(
                store.as_ref(),
                &p(key),
                Bytes::from_static(b"mine"),
                PutMode::Create,
                Verify::Body,
            )
            .await;
            assert!(out.is_err(), "{key}");
        }
    }

    // ---- the OVH run's finding 5, against the real S3 client ----

    /// A one-shot HTTP responder: answers the n-th request with
    /// `statuses[n]` (the last one repeats), recording each request's
    /// method. Enough of HTTP/1.1 for `object_store`'s S3 client.
    async fn scripted_s3(statuses: Vec<u16>) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            let mut n = 0usize;
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let status = statuses[n.min(statuses.len() - 1)];
                n += 1;
                let log = log.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    // Headers, then the body its Content-Length names.
                    let head_end = loop {
                        let Ok(k) = sock.read(&mut chunk).await else {
                            return;
                        };
                        if k == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..k]);
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let len: usize = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + len {
                        let Ok(k) = sock.read(&mut chunk).await else {
                            return;
                        };
                        if k == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..k]);
                    }
                    log.lock()
                        .unwrap()
                        .push(head.split(' ').next().unwrap_or("").to_string());
                    let (reason, body) = match status {
                        200 => ("OK", String::new()),
                        409 => (
                            "Conflict",
                            "<Error><Code>OperationAborted</Code><Message>A conflicting \
                             conditional operation is currently in progress against this \
                             resource.</Message></Error>"
                                .to_string(),
                        ),
                        412 => (
                            "Precondition Failed",
                            "<Error><Code>PreconditionFailed</Code></Error>".to_string(),
                        ),
                        _ => ("Error", String::new()),
                    };
                    let reply = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nETag: \"e1\"\r\n\
                         Connection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(reply.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (format!("http://{addr}"), seen)
    }

    fn real_s3(endpoint: &str) -> Arc<dyn ObjectStore> {
        Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_bucket_name("b")
                .with_region("us-east-1")
                .with_access_key_id("k")
                .with_secret_access_key("s")
                .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
                .with_retry(object_store::RetryConfig {
                    max_retries: 0,
                    retry_timeout: Duration::from_secs(5),
                    backoff: Default::default(),
                })
                .build()
                .unwrap(),
        )
    }

    /// The OVH run's finding 5: OVH answers a losing conditional write
    /// with 409 Conflict before settling on 412. The *real* S3 client's
    /// error for a 409 (not the fault injector's imitation) must classify
    /// as `Busy` on both a create and an update — never a lost race,
    /// never success — and a 412 as `Lost`.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_real_s3_clients_409_is_busy_and_412_is_lost() {
        let update = PutMode::Update(UpdateVersion {
            e_tag: Some("\"e0\"".into()),
            version: None,
        });
        for (status, mode, want) in [
            (409, PutMode::Create, CasCode::Busy),
            (409, update.clone(), CasCode::Busy),
            (412, PutMode::Create, CasCode::Lost),
            (412, update.clone(), CasCode::Lost),
        ] {
            let (endpoint, _) = scripted_s3(vec![status]).await;
            let store = real_s3(&endpoint);
            let err = store
                .put_opts(
                    &p("k"),
                    PutPayload::from_static(b"mine"),
                    PutOptions::from(mode.clone()),
                )
                .await
                .expect_err("scripted failure");
            assert_eq!(
                classify(&err, &mode),
                Some(want),
                "{status} on {mode:?}: {err}"
            );
        }
    }

    /// 409 then 412, as OVH answers a losing create: the attempt is
    /// retried after the 409 and the 412 decides it — a lost race, with
    /// no success and no hard error on the way. On a content-addressed
    /// create the 412 is the dedup hit (the object is there), never the
    /// 409.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_409_then_412_is_a_lost_race_not_success_or_an_error() {
        let (endpoint, seen) = scripted_s3(vec![409, 409, 412]).await;
        let store = real_s3(&endpoint);
        let out = put_conditional(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"mine"),
            PutMode::Create,
            Verify::Caller,
        )
        .await
        .unwrap();
        assert_eq!(out, CasPut::Lost);
        assert_eq!(*seen.lock().unwrap(), vec!["PUT", "PUT", "PUT"]);

        let (endpoint, seen) = scripted_s3(vec![409, 409, 409, 409, 409, 409, 409]).await;
        let store = real_s3(&endpoint);
        let out = put_conditional_with(
            store.as_ref(),
            &p("k"),
            Bytes::from_static(b"mine"),
            PutMode::Create,
            Verify::Caller,
            2,
        )
        .await;
        assert!(
            out.is_err(),
            "a 409 that never clears is the store's (transient) error: {out:?}"
        );
        assert_eq!(seen.lock().unwrap().len(), 3);

        // Content-addressed: 409 is retried, never a hit; 200 creates.
        let (endpoint, seen) = scripted_s3(vec![409, 200]).await;
        let store = real_s3(&endpoint);
        let existed = create_content_addressed(store.as_ref(), &p("c"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert!(!existed, "a 409 passed for a dedup hit");
        assert_eq!(seen.lock().unwrap().len(), 2);
    }
}
