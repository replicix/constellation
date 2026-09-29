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
    if let Some(rest) = url.strip_prefix("s3://") {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p.trim_matches('/')),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            bail!("missing bucket in {url:?}");
        }
        let (builder, resolution) = constellation_store_s3::amazon_s3_builder_resolved(bucket)
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

fn completed() {
    LAST_COMPLETED_MS.store(
        constellation_store_s3::lease::now_unix_ms(),
        Ordering::Relaxed,
    );
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
            LAST_UNANSWERED_MS.store(
                constellation_store_s3::lease::now_unix_ms(),
                Ordering::Relaxed,
            );
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

/// The counts so far (`status.s3`).
pub fn s3_request_counts() -> constellation_api::S3RequestStatus {
    let c = counts();
    constellation_api::S3RequestStatus {
        get: c.get.load(Ordering::Relaxed),
        head: c.head.load(Ordering::Relaxed),
        put: c.put.load(Ordering::Relaxed),
        list: c.list.load(Ordering::Relaxed),
        delete: c.delete.load(Ordering::Relaxed),
        copy: c.copy.load(Ordering::Relaxed),
        by_area: c.by_area.lock().unwrap().clone(),
        unanswered: UNANSWERED.load(Ordering::Relaxed),
        last_answered_unix_ms: LAST_COMPLETED_MS.load(Ordering::Relaxed),
        last_unanswered_unix_ms: LAST_UNANSWERED_MS.load(Ordering::Relaxed),
        last_unanswered_error: LAST_UNANSWERED_ERROR.lock().unwrap().clone(),
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
        let r = self.inner.put_opts(location, payload, opts).await;
        note(&r);
        r
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        count("PUT", Some(location));
        let r = self.inner.put_multipart_opts(location, opts).await;
        note(&r);
        r
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        count(if options.head { "HEAD" } else { "GET" }, Some(location));
        let r = self.inner.get_opts(location, options).await;
        note(&r);
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
        use futures::StreamExt;
        count("LIST", prefix);
        self.inner.list(prefix).inspect(note).boxed()
    }
    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        use futures::StreamExt;
        count("LIST", prefix);
        self.inner
            .list_with_offset(prefix, offset)
            .inspect(note)
            .boxed()
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        count("LIST", prefix);
        let r = self.inner.list_with_delimiter(prefix).await;
        note(&r);
        r
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        count("COPY", Some(to));
        let r = self.inner.copy_opts(from, to, options).await;
        note(&r);
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
        LAST_COMPLETED_MS.store(0, Ordering::Relaxed);
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
            url: "s3://bra-private/s3fs/x".into(),
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
        assert!(msg.contains("s3://bra-private/s3fs/x"), "{msg}");
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
