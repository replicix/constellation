//! Backend URL handling: `s3://bucket/prefix` for real object storage,
//! `file:///abs/path` or a plain directory path for local testing.

use anyhow::{bail, Context, Result};
use constellation_store_s3::{ChunkStore, FsMeta, MissingFs, StoreError};
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// Where a backend URL resolved to: the store a command actually talks
/// to, named in "no filesystem" errors (see [`load_fs_explained`]).
#[derive(Debug, Clone)]
pub struct BackendInfo {
    /// The URL as given (`s3://bucket/prefix`, or a local path).
    pub url: String,
    /// For `s3://`: the endpoint, region and credentials in use.
    pub s3: Option<constellation_store_s3::S3Resolution>,
}

impl BackendInfo {
    /// The endpoint requests go to (`None` for a local directory): what
    /// the registry remembers per name, to tell a later command that
    /// resolves a different one.
    pub fn endpoint(&self) -> Option<&str> {
        self.s3.as_ref().map(|r| r.endpoint.as_str())
    }
}

impl std::fmt::Display for BackendInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.s3 {
            Some(r) => write!(f, "{} ({r})", self.url),
            None => write!(f, "{}", self.url),
        }
    }
}

/// Build an object store scoped to the filesystem prefix from a URL.
///
/// For `s3://`, credentials come from the standard AWS SDK chain via
/// [`constellation_store_s3::amazon_s3_builder`] — env vars, shared
/// config/credentials files, `AWS_PROFILE`, SSO, IMDS, ECS/IRSA, etc.
pub async fn open_backend(url: &str) -> Result<Arc<dyn ObjectStore>> {
    Ok(open_backend_described(url).await?.0)
}

/// [`open_backend`], plus where it resolved to.
pub async fn open_backend_described(url: &str) -> Result<(Arc<dyn ObjectStore>, BackendInfo)> {
    open_backend_described_with(url, None).await
}

/// [`open_backend_described`] signing with an engine's own credential
/// source (plan 31 §9.8): `None`, or `CredentialSource::AwsDefaultChain`,
/// is the AWS SDK's chain as always; `Static`/`Refreshing` sign with
/// what the source resolves to, re-asked when a credential expires.
pub async fn open_backend_described_with(
    url: &str,
    credentials: Option<&Arc<constellation_platform::CredentialSource>>,
) -> Result<(Arc<dyn ObjectStore>, BackendInfo)> {
    let supplied = credentials
        .filter(|c| {
            !matches!(
                ***c,
                constellation_platform::CredentialSource::AwsDefaultChain
            )
        })
        .map(|c| {
            let provider: Arc<
                dyn object_store::CredentialProvider<Credential = object_store::aws::AwsCredential>,
            > = Arc::new(SourceCredentials {
                source: c.clone(),
                cached: tokio::sync::Mutex::new(None),
            });
            (provider, "the engine's credential source".to_string())
        });
    if let Some(rest) = url.strip_prefix("s3://") {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p.trim_matches('/')),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            bail!("missing bucket in {url:?}");
        }
        let (builder, resolution) =
            constellation_store_s3::amazon_s3_builder_resolved_with(bucket, supplied)
                .await
                .with_context(|| format!("resolving AWS credentials for {url:?}"))?;
        tracing::debug!(url, %resolution, "S3 backend resolved");
        let s3 = constellation_store_s3::configure_s3_client(builder)
            .build()
            .with_context(|| format!("building S3 client for {url:?}"))?;
        let info = BackendInfo {
            url: url.to_string(),
            s3: Some(resolution),
        };
        let s3: Arc<dyn ObjectStore> = if prefix.is_empty() {
            Arc::new(s3)
        } else {
            Arc::new(PrefixStore::new(s3, prefix))
        };
        Ok((Arc::new(CountingStore { inner: s3 }), info))
    } else {
        let path = url.strip_prefix("file://").unwrap_or(url);
        if !path.starts_with('/') {
            bail!("backend must be s3://bucket/prefix, file:///path, or an absolute path (got {url:?})");
        }
        std::fs::create_dir_all(path)
            .with_context(|| format!("creating local backend dir {path:?}"))?;
        let local = object_store::local::LocalFileSystem::new_with_prefix(path)
            .with_context(|| format!("opening local backend {path:?}"))?;
        let info = BackendInfo {
            url: url.to_string(),
            s3: None,
        };
        Ok((Arc::new(local.with_automatic_cleanup(true)), info))
    }
}

/// An engine's `CredentialSource` as object_store's credential provider:
/// resolved on first use, again once what it gave expires, and again when
/// the source changed under it — a static source's generation moves with
/// every `fs.unlock` rotation (plan 37 K6a), so the next request signs
/// with the new keys and no client is rebuilt. Each answer records its
/// generation as the one in use (`CredentialSource::note_in_use`), which
/// is what `fs.list` reports as the rotation the S3 clients have taken.
struct SourceCredentials {
    source: Arc<constellation_platform::CredentialSource>,
    cached: tokio::sync::Mutex<Option<CachedCredential>>,
}

struct CachedCredential {
    credential: Arc<object_store::aws::AwsCredential>,
    expiry: Option<std::time::SystemTime>,
    generation: u64,
}

impl std::fmt::Debug for SourceCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceCredentials").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl object_store::CredentialProvider for SourceCredentials {
    type Credential = object_store::aws::AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let mut cached = self.cached.lock().await;
        let now = std::time::SystemTime::now();
        if let Some(c) = cached.as_ref() {
            // Re-asked two minutes early, as the SDK adapter does.
            let fresh = c
                .expiry
                .is_none_or(|at| now + std::time::Duration::from_secs(120) < at);
            if fresh && c.generation == self.source.generation() {
                self.source.note_in_use(c.generation);
                return Ok(c.credential.clone());
            }
        }
        let (generation, resolved) =
            self.source
                .resolve_generation()
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(e),
                })?;
        let resolved = resolved.ok_or_else(|| object_store::Error::Generic {
            store: "S3",
            source: "the credential source defers to the AWS chain".into(),
        })?;
        let credential = Arc::new(object_store::aws::AwsCredential {
            key_id: resolved.access_key_id.clone(),
            secret_key: String::from_utf8_lossy(resolved.secret_access_key.expose()).into_owned(),
            token: resolved
                .session_token
                .as_ref()
                .map(|t| String::from_utf8_lossy(t.expose()).into_owned()),
        });
        *cached = Some(CachedCredential {
            credential: credential.clone(),
            expiry: resolved.expiry,
            generation,
        });
        self.source.note_in_use(generation);
        Ok(credential)
    }
}

/// Load `meta.json`; when it is missing, say where the command looked and
/// what it found there instead, so a command that reached the wrong store
/// (run without the `AWS_PROFILE` / `AWS_CONFIG_FILE` / `AWS_ENDPOINT_URL`
/// the filesystem was created with) says so rather than "no filesystem".
/// `registered` is the endpoint the registry remembers for the name, if
/// any.
pub async fn load_fs_explained(
    store: &Arc<dyn ObjectStore>,
    info: &BackendInfo,
    registered: Option<&str>,
) -> Result<FsMeta> {
    let chunks = ChunkStore::new(store.clone());
    match chunks.load_fs().await {
        Ok(meta) => Ok(meta),
        Err(StoreError::NotFound) => {
            let found = chunks.locate_missing_fs().await;
            Err(anyhow::anyhow!(missing_fs_message(
                info, &found, registered
            )))
        }
        Err(e) => Err(anyhow::Error::new(e).context(format!("reading meta.json at {info}"))),
    }
}

/// The text of [`load_fs_explained`]'s "no filesystem" error.
pub fn missing_fs_message(
    info: &BackendInfo,
    found: &MissingFs,
    registered: Option<&str>,
) -> String {
    let mut msg = format!(
        "no filesystem found at {}: meta.json is missing\n  looked at: {}\n  found: {found}",
        info.url,
        match &info.s3 {
            Some(r) => r.to_string(),
            None => "a local directory".to_string(),
        },
    );
    match (registered, info.endpoint()) {
        (Some(was), Some(now)) if was != now => msg.push_str(&format!(
            "\n  this name was created or last mounted against endpoint {was}, but this \
             command resolved {now}: run it with the same AWS_PROFILE / AWS_CONFIG_FILE / \
             AWS_ENDPOINT_URL environment the filesystem was created with"
        )),
        _ if *found == MissingFs::NoBucket => msg.push_str(
            "\n  check the AWS_PROFILE / AWS_CONFIG_FILE / AWS_ENDPOINT_URL environment this \
             command runs with (a `VAR=value cmd1 && cmd2` shell line sets VAR for cmd1 only)",
        ),
        _ => {}
    }
    msg
}

/// EC2 finding R2-2: every object-store request this daemon issues, by
/// kind and key area, for `status` (the tester could count sync rounds,
/// not requests).
#[derive(Default)]
struct RequestCounts {
    get: AtomicU64,
    head: AtomicU64,
    put: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
    by_area: std::sync::Mutex<std::collections::BTreeMap<String, u64>>,
}

fn counts() -> &'static RequestCounts {
    static COUNTS: std::sync::OnceLock<RequestCounts> = std::sync::OnceLock::new();
    COUNTS.get_or_init(RequestCounts::default)
}

fn count(kind: &'static str, location: Option<&object_store::path::Path>) {
    let c = counts();
    let n = match kind {
        "GET" => &c.get,
        "HEAD" => &c.head,
        "PUT" => &c.put,
        "LIST" => &c.list,
        "DELETE" => &c.delete,
        _ => &c.copy,
    };
    n.fetch_add(1, Ordering::Relaxed);
    let area = location
        .and_then(|l| l.as_ref().split('/').next().map(str::to_string))
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| "/".to_string());
    *c.by_area
        .lock()
        .unwrap()
        .entry(format!("{kind} {area}"))
        .or_default() += 1;
}

/// EC2 finding 1: when S3 last answered a request (unix ms; 0: never) —
/// a success, or an answer that is an error of the request (not found, a
/// failed precondition), which proves the path works as well. A
/// black-holed S3 path answers nothing: every request waits out its
/// timeouts and retries, then fails. A drain whose own uploads make no
/// progress hands its chunks to a peer only while this is old too, so
/// one slow upload on a slow but working link (the registry poll and the
/// sync rounds keep completing around it) is not mistaken for an outage.
static LAST_COMPLETED_MS: AtomicI64 = AtomicI64::new(0);

/// This node's wait clock (`ProtocolClock::wait`) in ms: what the S3 path's
/// stamps and the stall rule over them count on (plan 43 C7-H1). The
/// stall verdict is liveness, decided from this node's own elapsed time: a
/// wall step neither makes a healthy path look stalled (a forward one,
/// which would send the node's ops down the S3-less path and fail them at
/// its deadline) nor hides a stalled one (a backward one).
pub fn wait_ms() -> i64 {
    constellation_platform::clock::protocol().wait().0
}

/// A wait-clock stamp as unix ms for status (0, "never", stays 0).
fn status_unix_ms(wait_stamp: i64) -> i64 {
    if wait_stamp == 0 {
        0
    } else {
        let now = constellation_platform::clock::protocol().now();
        now.wall_of_wait(constellation_authority::WaitMs(wait_stamp))
            .0
    }
}

fn completed() {
    LAST_COMPLETED_MS.store(wait_ms(), Ordering::Relaxed);
}

/// Requests S3 did not answer, and the last one's time and error
/// (`status.s3`): with [`LAST_COMPLETED_MS`], what tells an S3 path that
/// fails now from a node that failed once and has not asked since.
static UNANSWERED: AtomicU64 = AtomicU64::new(0);
static LAST_UNANSWERED_MS: AtomicI64 = AtomicI64::new(0);
static LAST_UNANSWERED_ERROR: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Record a request's outcome: answered (success or a request error such
/// as not-found or a failed precondition, which proves the path works) or
/// not.
fn note<T>(r: &object_store::Result<T>) {
    use object_store::Error as E;
    match r {
        Ok(_)
        | Err(E::NotFound { .. })
        | Err(E::Precondition { .. })
        | Err(E::AlreadyExists { .. })
        | Err(E::NotModified { .. }) => completed(),
        Err(e) => {
            UNANSWERED.fetch_add(1, Ordering::Relaxed);
            LAST_UNANSWERED_MS.store(wait_ms(), Ordering::Relaxed);
            let mut text = e.to_string();
            text.truncate(400);
            *LAST_UNANSWERED_ERROR.lock().unwrap() = Some(text);
        }
    }
}

/// When S3 last answered a request (unix ms; 0: never).
pub fn last_s3_completion_ms() -> i64 {
    LAST_COMPLETED_MS.load(Ordering::Relaxed)
}

/// Finding 14: the requests S3 has been asked and has not answered yet,
/// by when each was issued (unix ms; keyed by a request number). The
/// stall test ([`s3_stall_in_ms`]) needs a request that waited: a daemon
/// that asked S3 nothing (its authority core blocked 18 s on a
/// metadata-store write, nothing else due) heard nothing either, and was
/// taken for one whose S3 path had stalled the moment it ran again. One
/// process-wide map behind a mutex, an insert and a remove per request:
/// noise next to the request itself.
static OUTSTANDING: std::sync::Mutex<std::collections::BTreeMap<u64, i64>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(0);

fn outstanding_requests() -> std::sync::MutexGuard<'static, std::collections::BTreeMap<u64, i64>> {
    OUTSTANDING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One request in [`OUTSTANDING`], from when it is issued until it is
/// settled (answered, or failed: [`Outstanding::settle`]) or given up. A
/// request its caller gave up before S3 answered (a timeout around it,
/// such as the epoch member probe's) went unanswered: dropping it
/// unsettled records that, as a failure would.
struct Outstanding {
    id: u64,
    settled: bool,
}

impl Outstanding {
    fn issued_at(at_ms: i64) -> Self {
        let id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        outstanding_requests().insert(id, at_ms);
        Self { id, settled: false }
    }

    fn issued() -> Self {
        Self::issued_at(wait_ms())
    }

    /// Record the request's outcome ([`note`]) and take it off the map.
    fn settle<T>(mut self, r: &object_store::Result<T>) {
        note(r);
        self.settled = true;
    }

    /// Taken off the map with no outcome of its own (a listing that
    /// ended or yielded its first item has its items noted one by one).
    fn settled(mut self) {
        self.settled = true;
    }
}

impl Drop for Outstanding {
    fn drop(&mut self) {
        outstanding_requests().remove(&self.id);
        if !self.settled {
            LAST_UNANSWERED_MS.store(wait_ms(), Ordering::Relaxed);
            *LAST_UNANSWERED_ERROR.lock().unwrap() =
                Some("given up by its caller before S3 answered".to_string());
        }
    }
}

/// A listing, outstanding until its first item, its end (an empty
/// listing is an answer too), or its drop; its items are noted one by
/// one as before.
fn listing<T: Send + 'static>(
    inner: futures::stream::BoxStream<'static, object_store::Result<T>>,
) -> futures::stream::BoxStream<'static, object_store::Result<T>> {
    use futures::StreamExt;
    let req = Outstanding::issued();
    futures::stream::unfold((inner, Some(req)), |(mut inner, mut req)| async move {
        match inner.next().await {
            Some(item) => {
                note(&item);
                if let Some(req) = req.take() {
                    req.settled();
                }
                Some((item, (inner, req)))
            }
            None => {
                if let Some(req) = req.take() {
                    completed();
                    req.settled();
                }
                None
            }
        }
    })
    .boxed()
}

/// EC2 finding 1, campaign 8 A-1: whether this node's S3 path has left
/// it waiting for `for_ms` at `now_ms` (`Some(0)`), how long until it
/// will have if nothing answers meanwhile (`Some(ms)`), or that nothing
/// was asked of it (`None`: no request waits, none failed unanswered
/// since the last answer — however long it was silent, finding 14).
///
/// S3 answered nothing for `for_ms` — since its last answer, or
/// `progress_ms` when the caller has later evidence of its own (a chunk
/// PUT that succeeded) — and it was asked meanwhile: a request has been
/// outstanding that long, or one failed unanswered (or was given up)
/// since the last answer (a request waiting out its retries' backoff
/// after a timeout has none outstanding).
///
/// What this cannot tell from S3: a daemon that itself stalled for a
/// while (its runtime held up, the answers that had arrived unread) has
/// its requests look outstanding all that time, and a retry series that
/// ran out during the stall fails unanswered right after it. The own-S3
/// watch counts ticks for the first (`OwnS3Watch`); the second can still
/// read as a stall until the next answer.
pub fn s3_stall_in_ms(now_ms: i64, for_ms: i64, progress_ms: i64) -> Option<i64> {
    S3Waits::now()
        .with_progress(progress_ms)
        .stall_in(now_ms, for_ms)
}

/// What [`s3_stall_in_ms`] judges by, read at one moment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct S3Waits {
    /// When S3 last answered (or the caller's later evidence of progress).
    pub answered_ms: i64,
    /// When the oldest request still waiting was issued.
    pub oldest_outstanding_ms: Option<i64>,
    /// When a request last failed unanswered or was given up.
    pub last_unanswered_ms: i64,
}

impl S3Waits {
    pub fn now() -> Self {
        Self {
            answered_ms: LAST_COMPLETED_MS.load(Ordering::Relaxed),
            oldest_outstanding_ms: outstanding_requests().values().min().copied(),
            last_unanswered_ms: LAST_UNANSWERED_MS.load(Ordering::Relaxed),
        }
    }

    pub fn with_progress(mut self, progress_ms: i64) -> Self {
        self.answered_ms = self.answered_ms.max(progress_ms);
        self
    }

    /// [`s3_stall_in_ms`].
    pub fn stall_in(&self, now_ms: i64, for_ms: i64) -> Option<i64> {
        stall_in(
            now_ms,
            for_ms,
            self.answered_ms,
            self.oldest_outstanding_ms,
            self.last_unanswered_ms,
        )
    }

    /// S3 left this node waiting all through `since_ms ..= now`: nothing
    /// answered since, and a request was already waiting then or one
    /// failed unanswered after the last answer.
    pub fn waited_since(&self, since_ms: i64) -> bool {
        self.answered_ms < since_ms
            && (self
                .oldest_outstanding_ms
                .is_some_and(|issued| issued <= since_ms)
                || self.last_unanswered_ms > self.answered_ms)
    }
}

/// [`s3_stall_in_ms`]'s rule.
fn stall_in(
    now_ms: i64,
    for_ms: i64,
    answered_ms: i64,
    oldest_outstanding_ms: Option<i64>,
    last_unanswered_ms: i64,
) -> Option<i64> {
    let silent = answered_ms + for_ms - now_ms;
    let failed = (last_unanswered_ms > answered_ms).then_some(silent);
    let waiting = oldest_outstanding_ms.map(|issued| silent.max(issued + for_ms - now_ms));
    let due = match (failed, waiting) {
        (Some(a), Some(b)) => a.min(b),
        (a, b) => a.or(b)?,
    };
    Some(due.max(0))
}

/// The oldest request still waiting on S3: when it was issued (unix ms),
/// and how many are waiting (`status.s3`).
pub fn s3_outstanding() -> (Option<i64>, usize) {
    let requests = outstanding_requests();
    (requests.values().min().copied(), requests.len())
}

/// Tests that set the S3 path's answers serialize on this: they are
/// process-wide.
#[cfg(test)]
pub(crate) fn s3_path_for_test(answered_ms: i64) -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let guard = LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    s3_reset_for_test(answered_ms);
    guard
}

/// Start the S3 path's record over (tests, under [`s3_path_for_test`]).
#[cfg(test)]
pub(crate) fn s3_reset_for_test(answered_ms: i64) {
    LAST_COMPLETED_MS.store(answered_ms, Ordering::Relaxed);
    LAST_UNANSWERED_MS.store(0, Ordering::Relaxed);
    outstanding_requests().clear();
}

/// A request issued at `at_ms` and outstanding until the guard drops,
/// answered then (tests).
#[cfg(test)]
pub(crate) fn s3_request_for_test(at_ms: i64) -> impl Drop {
    struct Answered(Option<Outstanding>);
    impl Drop for Answered {
        fn drop(&mut self) {
            if let Some(req) = self.0.take() {
                completed();
                req.settled();
            }
        }
    }
    Answered(Some(Outstanding::issued_at(at_ms)))
}

/// A request that went unanswered at `at_ms` (tests).
#[cfg(test)]
pub(crate) fn s3_unanswered_for_test(at_ms: i64) {
    LAST_UNANSWERED_MS.store(at_ms, Ordering::Relaxed);
}

/// The counts so far (`status.s3`).
pub fn s3_request_counts() -> constellation_control::proto::types::S3RequestStatus {
    let c = counts();
    let (oldest, outstanding) = s3_outstanding();
    constellation_control::proto::types::S3RequestStatus {
        get: c.get.load(Ordering::Relaxed),
        head: c.head.load(Ordering::Relaxed),
        put: c.put.load(Ordering::Relaxed),
        list: c.list.load(Ordering::Relaxed),
        delete: c.delete.load(Ordering::Relaxed),
        copy: c.copy.load(Ordering::Relaxed),
        by_area: c.by_area.lock().unwrap().clone(),
        unanswered: UNANSWERED.load(Ordering::Relaxed),
        last_answered_unix_ms: status_unix_ms(LAST_COMPLETED_MS.load(Ordering::Relaxed)),
        last_unanswered_unix_ms: status_unix_ms(LAST_UNANSWERED_MS.load(Ordering::Relaxed)),
        last_unanswered_error: LAST_UNANSWERED_ERROR.lock().unwrap().clone(),
        outstanding: outstanding as u64,
        oldest_outstanding_ms: oldest.map(|issued| (wait_ms() - issued).max(0) as u64),
    }
}

/// An S3 backend that counts what it is asked (`RequestCounts`).
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        count("PUT", Some(location));
        let req = Outstanding::issued();
        let r = self.inner.put_opts(location, payload, opts).await;
        req.settle(&r);
        r
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        count("PUT", Some(location));
        let req = Outstanding::issued();
        let r = self.inner.put_multipart_opts(location, opts).await;
        req.settle(&r);
        r
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        count(if options.head { "HEAD" } else { "GET" }, Some(location));
        let req = Outstanding::issued();
        let r = self.inner.get_opts(location, options).await;
        req.settle(&r);
        r
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        use futures::StreamExt;
        self.inner.delete_stream(
            locations
                .inspect(|l| {
                    if let Ok(l) = l {
                        count("DELETE", Some(l));
                    }
                })
                .boxed(),
        )
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        count("LIST", prefix);
        listing(self.inner.list(prefix))
    }
    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        count("LIST", prefix);
        listing(self.inner.list_with_offset(prefix, offset))
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        count("LIST", prefix);
        let req = Outstanding::issued();
        let r = self.inner.list_with_delimiter(prefix).await;
        req.settle(&r);
        r
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        count("COPY", Some(to));
        let req = Outstanding::issued();
        let r = self.inner.copy_opts(from, to, options).await;
        req.settle(&r);
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An answer — a 404 included — is progress; a transport failure is
    /// not.
    #[test]
    fn a_request_error_counts_as_an_answer() {
        let _path = s3_path_for_test(0);
        note::<()>(&Err(object_store::Error::Generic {
            store: "S3",
            source: "error sending request".into(),
        }));
        assert_eq!(last_s3_completion_ms(), 0);
        note::<()>(&Err(object_store::Error::NotFound {
            path: "x".into(),
            source: "404".into(),
        }));
        assert!(last_s3_completion_ms() > 0);
    }

    /// Finding 14: S3 is stalled only while it leaves a request waiting.
    /// A daemon that asked nothing for a minute (its authority core held
    /// up by its metadata store) is not; a request outstanding past the
    /// window is, and so is one that failed unanswered since the last
    /// answer (the next attempt has not gone out yet); an answer, or the
    /// caller's own evidence of progress, restarts the window. Not yet
    /// stalled, the rule says when it will be if nothing answers.
    #[test]
    fn s3_is_stalled_only_while_a_request_waits_unanswered() {
        let w = 6_000;
        let now = 1_000_000;
        let long_ago = now - 60_000;
        // Nothing asked: silence is no stall, and none is due.
        assert_eq!(stall_in(now, w, long_ago, None, 0), None);
        // A request that has waited the whole window.
        assert_eq!(stall_in(now, w, long_ago, Some(now - w), 0), Some(0));
        // One that went out 100 ms ago: due when it has waited 6 s.
        assert_eq!(stall_in(now, w, long_ago, Some(now - 100), 0), Some(5_900));
        // A request that failed unanswered after the last answer.
        assert_eq!(stall_in(now, w, long_ago, None, now - 2_000), Some(0));
        // ... but not one that failed before it.
        assert_eq!(stall_in(now, w, long_ago, None, long_ago - 1), None);
        // Something answered 100 ms ago: a slow request on a working path
        // is no stall, not before the window has passed since the answer.
        assert_eq!(
            stall_in(now, w, now - 100, Some(now - 30_000), now - 50),
            Some(5_900)
        );
    }

    /// The same through the process-wide record the requests keep; a
    /// request given up unanswered (its guard dropped unsettled) counts
    /// as one that failed.
    #[test]
    fn the_outstanding_requests_decide_the_stall() {
        let now = wait_ms();
        let _path = s3_path_for_test(now - 60_000);
        assert_eq!(s3_stall_in_ms(now, 6_000, 0), None, "nothing asked");
        {
            let _waiting = s3_request_for_test(now - 7_000);
            let _fresh = s3_request_for_test(now - 100);
            assert_eq!(s3_outstanding(), (Some(now - 7_000), 2));
            assert_eq!(s3_stall_in_ms(now, 6_000, 0), Some(0), "waited 7 s");
            // The caller's own evidence of progress restarts the window.
            assert_eq!(s3_stall_in_ms(now, 6_000, now - 1_000), Some(5_000));
        }
        // Answered: the window counts from the answer.
        assert_eq!(s3_outstanding(), (None, 0));
        assert_eq!(s3_stall_in_ms(now, 6_000, 0), None, "answered");
        s3_reset_for_test(now - 60_000);
        drop(Outstanding::issued_at(now - 300));
        assert_eq!(s3_stall_in_ms(now + 1, 6_000, 0), Some(0), "given up");
        s3_reset_for_test(now - 60_000);
        s3_unanswered_for_test(now - 1_000);
        assert_eq!(s3_stall_in_ms(now, 6_000, 0), Some(0), "failed unanswered");
    }

    /// A listing is outstanding until its first item or its end, an empty
    /// one being an answer too.
    #[test]
    fn a_listing_is_answered_by_its_first_item_or_its_end() {
        use futures::StreamExt;
        let now = wait_ms();
        let _path = s3_path_for_test(now - 60_000);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let empty = futures::stream::empty::<object_store::Result<()>>().boxed();
            let mut l = listing(empty);
            assert_eq!(s3_outstanding().1, 1);
            assert!(l.next().await.is_none());
            assert_eq!(s3_outstanding().1, 0);
            assert!(last_s3_completion_ms() >= now);
            drop(l);
            s3_reset_for_test(now - 60_000);
            let two = futures::stream::iter([Ok(()), Ok(())]).boxed();
            let mut l = listing(two);
            assert!(l.next().await.is_some());
            assert_eq!(s3_outstanding().1, 0, "answered by the first item");
            drop(l);
            assert_eq!(s3_stall_in_ms(now, 6_000, 0), None, "not given up");
        });
    }

    /// Plan 37 K6a: an `fs.unlock` rotation reaches the next signed
    /// request through the same provider (no client rebuilt), the old key
    /// is never handed out again, and the generation in use follows.
    #[tokio::test]
    async fn a_rotated_static_source_signs_the_next_request_with_the_new_key() {
        use constellation_platform::{CredentialSource, EphemeralSecretStore};
        use object_store::CredentialProvider;
        let store = EphemeralSecretStore::new();
        let pair = |id: &[u8], secret: &[u8]| {
            store
                .replace(&[
                    (CredentialSource::ACCESS_KEY_ID, Some(id)),
                    (CredentialSource::SECRET_ACCESS_KEY, Some(secret)),
                ])
                .unwrap()
        };
        pair(b"KEY-A", b"secret-a");
        let source = Arc::new(CredentialSource::Static(store.clone()));
        let provider = SourceCredentials {
            source: source.clone(),
            cached: tokio::sync::Mutex::new(None),
        };
        for _ in 0..3 {
            assert_eq!(provider.get_credential().await.unwrap().key_id, "KEY-A");
        }
        assert_eq!(store.in_use_generation(), 1);
        pair(b"KEY-B", b"secret-b");
        assert_eq!(store.in_use_generation(), 1, "nothing signed since");
        let next = provider.get_credential().await.unwrap();
        assert_eq!(
            (next.key_id.as_str(), next.secret_key.as_str()),
            ("KEY-B", "secret-b")
        );
        assert_eq!(store.in_use_generation(), 2);
        for _ in 0..3 {
            assert_eq!(provider.get_credential().await.unwrap().key_id, "KEY-B");
        }
    }

    #[tokio::test]
    async fn local_paths_accepted() {
        let dir = tempfile::TempDir::new().unwrap();
        let p = dir.path().to_str().unwrap().to_string();
        assert!(open_backend(&p).await.is_ok());
        assert!(open_backend(&format!("file://{p}")).await.is_ok());
        assert!(open_backend("relative/path").await.is_err());
    }

    fn aws_default() -> BackendInfo {
        BackendInfo {
            url: "s3://test-bucket/s3fs/x".into(),
            s3: Some(constellation_store_s3::S3Resolution {
                endpoint: "https://s3.us-west-2.amazonaws.com".into(),
                endpoint_configured: false,
                region: "us-west-2".into(),
                credentials: Some("IMDSv2".into()),
                profile: None,
            }),
        }
    }

    /// The OVH campaigns' failing `mount`: no OVH profile in its
    /// environment, so it asked AWS. The error names the endpoint, the
    /// credentials, the missing bucket, and the endpoint the name was
    /// created against.
    #[test]
    fn a_mount_that_reached_the_wrong_store_says_so() {
        let info = aws_default();
        let msg = missing_fs_message(
            &info,
            &MissingFs::NoBucket,
            Some("https://s3.eu-south-mil.io.cloud.ovh.net"),
        );
        assert!(msg.contains("s3://test-bucket/s3fs/x"), "{msg}");
        assert!(
            msg.contains("https://s3.us-west-2.amazonaws.com (AWS default"),
            "{msg}"
        );
        assert!(msg.contains("credentials from IMDSv2"), "{msg}");
        assert!(msg.contains("bucket does not exist"), "{msg}");
        assert!(
            msg.contains("last mounted against endpoint https://s3.eu-south-mil"),
            "{msg}"
        );
        // Unregistered: the missing bucket alone points at the environment.
        let msg = missing_fs_message(&info, &MissingFs::NoBucket, None);
        assert!(msg.contains("AWS_PROFILE / AWS_CONFIG_FILE"), "{msg}");
        // Same endpoint, empty prefix: a plain "fs create first".
        let msg = missing_fs_message(
            &info,
            &MissingFs::EmptyPrefix,
            Some("https://s3.us-west-2.amazonaws.com"),
        );
        assert!(msg.contains("fs create"), "{msg}");
        assert!(!msg.contains("AWS_PROFILE /"), "{msg}");
    }

    #[tokio::test]
    async fn a_local_prefix_without_meta_json_is_explained() {
        let dir = tempfile::TempDir::new().unwrap();
        let p = dir.path().to_str().unwrap().to_string();
        let (store, info) = open_backend_described(&p).await.unwrap();
        let err = load_fs_explained(&store, &info, None).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("meta.json is missing"), "{msg}");
        assert!(msg.contains("holds nothing under this prefix"), "{msg}");
    }
}
