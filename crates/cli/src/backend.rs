//! Backend URL handling: `s3://bucket/prefix` for real object storage,
//! `file:///abs/path` or a plain directory path for local testing.

use anyhow::{bail, Context, Result};
use constellation_store_s3::{ChunkStore, FsMeta, MissingFs, StoreError};
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
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
        let mut retry = object_store::RetryConfig::default();
        if let Ok(n) = std::env::var("CONSTELLATION_S3_MAX_RETRIES") {
            if let Ok(n) = n.parse() {
                retry.max_retries = n;
            }
        }
        if let Ok(ms) = std::env::var("CONSTELLATION_S3_RETRY_TIMEOUT_MS") {
            if let Ok(ms) = ms.parse() {
                retry.retry_timeout = std::time::Duration::from_millis(ms);
            }
        }
        let (builder, resolution) = constellation_store_s3::amazon_s3_builder_resolved(bucket)
            .await
            .with_context(|| format!("resolving AWS credentials for {url:?}"))?;
        tracing::debug!(url, %resolution, "S3 backend resolved");
        let s3 = builder
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .with_retry(retry)
            .build()
            .with_context(|| format!("building S3 client for {url:?}"))?;
        let info = BackendInfo {
            url: url.to_string(),
            s3: Some(resolution),
        };
        if prefix.is_empty() {
            Ok((Arc::new(s3), info))
        } else {
            Ok((Arc::new(PrefixStore::new(s3, prefix)), info))
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
