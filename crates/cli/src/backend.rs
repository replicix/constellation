//! Backend URL handling: `s3://bucket/prefix` for real object storage,
//! `file:///abs/path` or a plain directory path for local testing.

use anyhow::{bail, Context, Result};
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use std::sync::Arc;

/// Build an object store scoped to the filesystem prefix from a URL.
pub fn open_backend(url: &str) -> Result<Arc<dyn ObjectStore>> {
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
        let s3 = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .with_retry(retry)
            .build()
            .with_context(|| format!("building S3 client for {url:?}"))?;
        if prefix.is_empty() {
            Ok(Arc::new(s3))
        } else {
            Ok(Arc::new(PrefixStore::new(s3, prefix)))
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
        Ok(Arc::new(local.with_automatic_cleanup(true)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_paths_accepted() {
        let dir = tempfile::TempDir::new().unwrap();
        let p = dir.path().to_str().unwrap().to_string();
        assert!(open_backend(&p).is_ok());
        assert!(open_backend(&format!("file://{p}")).is_ok());
        assert!(open_backend("relative/path").is_err());
    }
}
