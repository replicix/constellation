//! Real S3 upload target: `object_store`'s S3 backend, targeting a
//! caller-supplied `s3://bucket/prefix` (the benchmark's own run gets a
//! unique sub-prefix so concurrent runs and repeated invocations never
//! collide, and so cleanup can be scoped precisely).

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};

pub struct S3Target {
    store: Arc<dyn ObjectStore>,
    /// Unique per-run prefix under the caller's bucket/prefix, e.g.
    /// `benchmark-prefix/aimd-20260902-153000/`.
    run_prefix: StorePath,
}

impl S3Target {
    /// `url` is `s3://bucket/optional/prefix`. `run_label` (e.g. the
    /// controller name plus a timestamp) becomes a sub-prefix so every
    /// run's objects are trivially distinguishable and independently
    /// cleanable. `max_retries` should normally be 0-1: uploadbench
    /// wants to see real errors itself rather than have object_store's
    /// built-in retry hide them from the controller under test.
    ///
    /// Credentials use the standard AWS SDK chain (env, `AWS_PROFILE`,
    /// SSO, IMDS, …) via [`constellation_store_s3::amazon_s3_builder`].
    pub async fn new(url: &str, run_label: &str, max_retries: usize) -> Result<Self> {
        let Some(rest) = url.strip_prefix("s3://") else {
            bail!("expected s3://bucket[/prefix], got {url:?}");
        };
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p.trim_matches('/')),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            bail!("missing bucket in {url:?}");
        }
        let retry = RetryConfig {
            max_retries,
            ..RetryConfig::default()
        };
        let s3 = constellation_store_s3::amazon_s3_builder(bucket)
            .await
            .with_context(|| format!("resolving AWS credentials for {url:?}"))?
            .with_retry(retry)
            .build()
            .with_context(|| format!("building S3 client for {url:?}"))?;
        let run_prefix = if prefix.is_empty() {
            StorePath::from(run_label)
        } else {
            StorePath::from(format!("{prefix}/{run_label}"))
        };
        Ok(Self {
            store: Arc::new(s3),
            run_prefix,
        })
    }

    /// Upload `size` random-ish bytes under this run's prefix. Returns
    /// wall-clock latency on success, or the underlying `object_store`
    /// error (already retried up to `max_retries` times internally, per
    /// [`Self::new`]) on failure.
    pub async fn put(&self, seq: u64, body: Bytes) -> Result<Duration, object_store::Error> {
        let key = self.run_prefix.clone().join(format!("obj-{seq:012x}"));
        let started = Instant::now();
        self.store.put(&key, PutPayload::from_bytes(body)).await?;
        Ok(started.elapsed())
    }

    /// Delete every object this run created. Best-effort: logs and
    /// keeps going on individual delete failures so one straggler
    /// doesn't strand the rest.
    pub async fn cleanup(&self) -> Result<usize> {
        let mut deleted = 0usize;
        let mut listing = self.store.list(Some(&self.run_prefix));
        while let Some(meta) = listing.try_next().await? {
            match self.store.delete(&meta.location).await {
                Ok(()) => deleted += 1,
                Err(error) => {
                    tracing::warn!(location = %meta.location, %error, "cleanup: failed to delete object");
                }
            }
        }
        Ok(deleted)
    }
}

/// Best-effort classification of whether an `object_store` error looks
/// like an S3 throttling response (503 SlowDown / 429-style rate
/// limiting) as opposed to some other failure. `object_store`'s AWS
/// backend doesn't expose a dedicated variant for this, so we pattern
/// match on the rendered error text, which is the same approach
/// `constellation`'s own retry loop would need if it ever wanted to
/// treat throttling specially.
pub fn looks_like_slowdown(error: &object_store::Error) -> bool {
    let text = error.to_string();
    text.contains("SlowDown")
        || text.contains("503")
        || text.contains("TooManyRequests")
        || text.contains("RequestLimitExceeded")
        || text.contains("Throttl")
}
