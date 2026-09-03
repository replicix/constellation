//! Bridge the AWS SDK default credential chain into `object_store`.
//!
//! `AmazonS3Builder::from_env()` only understands static `AWS_ACCESS_KEY_*`
//! (plus IMDS/ECS/web-identity). It does **not** read `AWS_PROFILE`,
//! `~/.aws/credentials`, `~/.aws/config`, or SSO caches. The official
//! [`aws_config`] crate does — the same chain the AWS CLI and SDKs use —
//! so we load that chain once and hand object_store a
//! [`CredentialProvider`] that refreshes through it for the life of the
//! process (SSO session tokens expire; static env keys do not).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::CredentialProvider;
use tokio::sync::RwLock;

use crate::error::StoreError;

/// Refresh a little before the provider's stated expiry so a request in
/// flight near the edge does not race a just-expired token.
const EXPIRY_SKEW: Duration = Duration::from_secs(120);

#[derive(Debug)]
struct CachedCreds {
    data: Arc<AwsCredential>,
    /// `None` means non-expiring (static keys).
    expires_at: Option<SystemTime>,
}

impl CachedCreds {
    fn from_sdk(creds: &aws_credential_types::Credentials) -> Self {
        Self {
            data: Arc::new(AwsCredential {
                key_id: creds.access_key_id().to_owned(),
                secret_key: creds.secret_access_key().to_owned(),
                token: creds.session_token().map(str::to_owned),
            }),
            expires_at: creds.expiry(),
        }
    }

    fn still_valid(&self) -> bool {
        match self.expires_at {
            None => true,
            Some(at) => SystemTime::now() + EXPIRY_SKEW < at,
        }
    }
}

/// object_store adapter around [`SharedCredentialsProvider`], with a
/// small local cache so every PUT does not re-hit SSO.
#[derive(Debug)]
struct SdkCredentialProvider {
    provider: SharedCredentialsProvider,
    cache: RwLock<Option<CachedCreds>>,
}

impl SdkCredentialProvider {
    async fn fetch(&self) -> object_store::Result<CachedCreds> {
        let creds = self.provider.provide_credentials().await.map_err(|source| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(source),
            }
        })?;
        Ok(CachedCreds::from_sdk(&creds))
    }
}

#[async_trait]
impl CredentialProvider for SdkCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        if let Some(cached) = self.cache.read().await.as_ref() {
            if cached.still_valid() {
                return Ok(cached.data.clone());
            }
        }
        let mut guard = self.cache.write().await;
        if let Some(cached) = guard.as_ref() {
            if cached.still_valid() {
                return Ok(cached.data.clone());
            }
        }
        let fresh = self.fetch().await?;
        let data = fresh.data.clone();
        *guard = Some(fresh);
        Ok(data)
    }
}

/// Build an [`AmazonS3Builder`] for `bucket` using the standard AWS
/// credential chain (env vars, shared config/credentials files,
/// `AWS_PROFILE`, SSO, IMDS, ECS/IRSA, process credentials, …).
///
/// Still calls [`AmazonS3Builder::from_env`] so harness/localstack knobs
/// (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, …) keep working; credentials and
/// region from the SDK override the builder's limited env parsing.
pub async fn amazon_s3_builder(bucket: &str) -> Result<AmazonS3Builder, StoreError> {
    let sdk = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let provider = sdk.credentials_provider().ok_or_else(|| {
        StoreError::AwsCredentials(
            "credential provider missing from SDK config \
             (no env keys, profile, SSO, or instance role found)"
                .into(),
        )
    })?;

    let adapter = SdkCredentialProvider {
        provider,
        cache: RwLock::new(None),
    };
    // Fail fast with a clear message (expired SSO, missing profile, …)
    // instead of on the first PUT minutes later.
    adapter.get_credential().await.map_err(|source| {
        StoreError::AwsCredentials(format!(
            "loading via the standard chain \
             (env, ~/.aws profile/SSO, IMDS, …): {source}"
        ))
    })?;

    let region = sdk
        .region()
        .map(|r| r.as_ref().to_string())
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".into());

    Ok(AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_credentials(Arc::new(adapter)))
}
