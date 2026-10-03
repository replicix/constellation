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
use aws_types::service_config::ServiceConfigKey;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential};
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
        let creds = self
            .provider
            .provide_credentials()
            .await
            .map_err(|source| object_store::Error::Generic {
                store: "S3",
                source: Box::new(source),
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

/// Where an S3 client built by [`amazon_s3_builder_resolved`] sends its
/// requests and who it signs them as: named in the errors a missing
/// filesystem reports, because a command run without the environment the
/// filesystem was created with (`AWS_PROFILE`, `AWS_CONFIG_FILE`,
/// `AWS_ENDPOINT_URL`) silently talks to a different store — the OVH
/// campaigns' "mount lag" was exactly that (a scripted `mount` went to
/// AWS with the instance role and found no bucket there).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Resolution {
    /// The endpoint requests go to: the configured one, or AWS's
    /// regional default.
    pub endpoint: String,
    /// Whether `endpoint` was configured (profile, `services` stanza or
    /// environment) rather than AWS's default.
    pub endpoint_configured: bool,
    pub region: String,
    /// The link of the credential chain that answered ("ProfileFile",
    /// "IMDSv2", "Environment", ...), when the SDK names it.
    pub credentials: Option<String>,
    /// `AWS_PROFILE`, when set (else the SDK's default profile).
    pub profile: Option<String>,
}

impl std::fmt::Display for S3Resolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "endpoint {}", self.endpoint)?;
        if !self.endpoint_configured {
            write!(f, " (AWS default: no endpoint configured)")?;
        }
        write!(f, ", region {}", self.region)?;
        if let Some(c) = &self.credentials {
            write!(f, ", credentials from {c}")?;
        }
        match &self.profile {
            Some(p) => write!(f, ", AWS_PROFILE={p}"),
            None => write!(f, ", AWS_PROFILE unset"),
        }
    }
}

/// The provider name the SDK stamps on credentials. `Credentials` has no
/// getter for it; its `Debug` (which redacts the secret) prints it. Only
/// that field is extracted — the rest of the `Debug` text is never kept.
fn provider_name(creds: &aws_credential_types::Credentials) -> Option<String> {
    let debug = format!("{creds:?}");
    let rest = &debug[debug.find("provider_name: \"")? + "provider_name: \"".len()..];
    let name = &rest[..rest.find('"')?];
    (!name.is_empty()).then(|| name.to_string())
}

/// Build an [`AmazonS3Builder`] for `bucket` using the standard AWS
/// credential chain (env vars, shared config/credentials files,
/// `AWS_PROFILE`, SSO, IMDS, ECS/IRSA, process credentials, …).
///
/// Still calls [`AmazonS3Builder::from_env`] so harness/localstack knobs
/// (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, …) keep working; credentials and
/// region from the SDK override the builder's limited env parsing.
pub async fn amazon_s3_builder(bucket: &str) -> Result<AmazonS3Builder, StoreError> {
    Ok(amazon_s3_builder_resolved(bucket).await?.0)
}

/// [`amazon_s3_builder`], plus where the client will send its requests.
pub async fn amazon_s3_builder_resolved(
    bucket: &str,
) -> Result<(AmazonS3Builder, S3Resolution), StoreError> {
    amazon_s3_builder_resolved_with(bucket, None).await
}

/// Credentials an engine supplies itself (plan 31 §9.8's `Static` and
/// `Refreshing` sources) instead of the SDK's chain, and how `status`
/// names them.
pub type SuppliedCredentials = (
    Arc<dyn CredentialProvider<Credential = AwsCredential>>,
    String,
);

/// [`amazon_s3_builder_resolved`], signing with `supplied` credentials
/// when given: the SDK's configuration still decides the region and the
/// endpoint, but its credential chain is not consulted (and need not
/// find anything).
pub async fn amazon_s3_builder_resolved_with(
    bucket: &str,
    supplied: Option<SuppliedCredentials>,
) -> Result<(AmazonS3Builder, S3Resolution), StoreError> {
    let sdk = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let (provider, credentials): (
        Arc<dyn CredentialProvider<Credential = AwsCredential>>,
        Option<String>,
    ) = match supplied {
        Some((provider, name)) => {
            // An engine's own source is re-asked whenever what it gave
            // expires (`CredentialSource`).
            crate::classify::set_refreshable_credentials(true);
            (provider, Some(name))
        }
        None => {
            let provider = sdk.credentials_provider().ok_or_else(|| {
                StoreError::AwsCredentials(
                    "credential provider missing from SDK config \
                     (no env keys, profile, SSO, or instance role found)"
                        .into(),
                )
            })?;

            // Fail fast with a clear message (expired SSO, missing
            // profile, …) instead of on the first PUT minutes later; the
            // answer seeds the adapter's cache.
            let first = provider.provide_credentials().await.map_err(|source| {
                StoreError::AwsCredentials(format!(
                    "loading via the standard chain \
                     (env, ~/.aws profile/SSO, IMDS, …): {source}"
                ))
            })?;
            let credentials = provider_name(&first);
            // Plan 39: an `ExpiredToken` is worth waiting out only when
            // the chain hands out expiring credentials it renews.
            crate::classify::set_refreshable_credentials(first.expiry().is_some());
            let adapter = SdkCredentialProvider {
                provider,
                cache: RwLock::new(Some(CachedCreds::from_sdk(&first))),
            };
            (Arc::new(adapter), credentials)
        }
    };

    let region = sdk
        .region()
        .map(|r| r.as_ref().to_string())
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".into());

    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(region.clone())
        .with_credentials(provider);

    // `AmazonS3Builder::from_env` reads `AWS_ENDPOINT` (object_store's own
    // env var) but not `AWS_ENDPOINT_URL` / `AWS_ENDPOINT_URL_S3` (standard
    // AWS CLI variables). The SDK's `SdkConfig::endpoint_url()` covers the
    // profile-level `endpoint_url` key and `AWS_ENDPOINT_URL`, but NOT the
    // newer `services` stanza used by non-AWS stores (Backblaze B2, etc.).
    //
    // `SdkConfig::service_config()` + `ServiceConfigKey` does handle all of
    // that — it checks `AWS_ENDPOINT_URL_S3`, the profile-level key, and
    // the `services = name` / `[services name]` / `s3.endpoint_url` chain.
    let endpoint = sdk
        .service_config()
        .and_then(|sc| {
            ServiceConfigKey::builder()
                .service_id("S3")
                .env("AWS_ENDPOINT_URL_S3")
                .profile("endpoint_url")
                .build()
                .ok()
                .and_then(|key| sc.load_config(key))
        })
        .or_else(|| sdk.endpoint_url().map(str::to_owned));
    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep);
    }

    let configured = builder.get_config_value(&AmazonS3ConfigKey::Endpoint);
    let resolution = S3Resolution {
        endpoint_configured: configured.is_some(),
        endpoint: configured.unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com")),
        region,
        credentials,
        profile: std::env::var("AWS_PROFILE").ok().filter(|p| !p.is_empty()),
    };
    Ok((builder, resolution))
}

/// The request policy every S3 client of a daemon or command gets, on
/// top of where it points and who it signs as: conditional PUTs by ETag,
/// and a bounded retry budget.
///
/// EC2 finding 1: object_store's default retry budget (180 s) put up to
/// three minutes of retries on an unreachable S3 behind every request —
/// including the ones a FUSE operation waits on (a chunk no peer has, a
/// write-through upload no peer can take over). Every caller has its own
/// retry loop (sync rounds, uploads, lease renewals), so one request
/// series gives up after 30 s (`CONSTELLATION_S3_RETRY_TIMEOUT_MS`,
/// `CONSTELLATION_S3_MAX_RETRIES` override it).
pub fn configure_s3_client(builder: AmazonS3Builder) -> AmazonS3Builder {
    let mut retry = object_store::RetryConfig {
        retry_timeout: Duration::from_secs(30),
        ..object_store::RetryConfig::default()
    };
    if let Some(n) = std::env::var("CONSTELLATION_S3_MAX_RETRIES")
        .ok()
        .and_then(|n| n.parse().ok())
    {
        retry.max_retries = n;
    }
    if let Some(ms) = std::env::var("CONSTELLATION_S3_RETRY_TIMEOUT_MS")
        .ok()
        .and_then(|ms| ms.parse().ok())
    {
        retry.retry_timeout = Duration::from_millis(ms);
    }
    builder
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .with_retry(retry)
        .with_http_connector(RedactingConnector)
}

/// The HTTP connector of every S3 client [`configure_s3_client`] builds:
/// reqwest's, with each error response's body cut down to its S3 error
/// code before object_store sees it.
///
/// **Why** (plan 37 K6a): object_store puts an error response's body into
/// its error text, and an S3 authentication failure's body echoes the
/// request — `<AWSAccessKeyId>`, `<StringToSign>`, `<CanonicalRequest>`
/// (AWS, versitygw, MinIO all do). That text reaches the daemon's log, a
/// control error, a CSI plugin's gRPC status and from there a kubelet event
/// on a tenant's pod. Rewritten here, once, it cannot reach any of them:
/// the body that is left is `<Error><Code>…</Code><Message>…</Message></Error>`
/// with the code (what [`crate::classify`] decides by) and a fixed message
/// — for 401/403, that S3 refused the credentials. The status line is
/// untouched. Bodies of successful responses are never read here.
#[derive(Debug, Default, Clone, Copy)]
pub struct RedactingConnector;

impl object_store::client::HttpConnector for RedactingConnector {
    fn connect(
        &self,
        options: &object_store::ClientOptions,
    ) -> object_store::Result<object_store::client::HttpClient> {
        let inner = object_store::client::ReqwestConnector::default().connect(options)?;
        Ok(object_store::client::HttpClient::new(RedactingService(
            inner,
        )))
    }
}

#[derive(Debug)]
struct RedactingService(object_store::client::HttpClient);

#[async_trait::async_trait]
impl object_store::client::HttpService for RedactingService {
    async fn call(
        &self,
        req: object_store::client::HttpRequest,
    ) -> Result<object_store::client::HttpResponse, object_store::client::HttpError> {
        let response = self.0.execute(req).await?;
        let status = response.status().as_u16();
        if status < 400 {
            return Ok(response);
        }
        let (mut parts, body) = response.into_parts();
        let body = read_capped(body.bytes_stream(), ERROR_BODY_CAP).await?;
        let redacted = redacted_error_body(status, &String::from_utf8_lossy(&body));
        parts.headers.remove("content-length");
        Ok(object_store::client::HttpResponse::from_parts(
            parts,
            redacted.into(),
        ))
    }
}

/// How much of an error response's body is read: S3 puts the `<Code>`
/// first, and only the code survives.
const ERROR_BODY_CAP: usize = 64 * 1024;

/// The first `cap` bytes of `stream`; the rest is not read.
async fn read_capped(
    mut stream: futures::stream::BoxStream<
        'static,
        Result<bytes::Bytes, object_store::client::HttpError>,
    >,
    cap: usize,
) -> Result<Vec<u8>, object_store::client::HttpError> {
    use futures::StreamExt;
    let mut body = Vec::new();
    while body.len() < cap {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk?;
        let room = cap - body.len();
        body.extend_from_slice(&chunk[..chunk.len().min(room)]);
    }
    Ok(body)
}

/// What [`RedactingConnector`] leaves of an error response's `body`: its
/// `<Code>` (restricted to an S3 error code's alphabet) and a fixed
/// message. Never anything else of the body.
pub fn redacted_error_body(status: u16, body: &str) -> String {
    let code = body
        .split_once("<Code>")
        .and_then(|(_, rest)| rest.split_once("</Code>"))
        .map(|(code, _)| code.trim())
        .filter(|c| {
            !c.is_empty()
                && c.len() <= 64
                && c.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        });
    let message = match status {
        401 | 403 => "S3 refused the credentials (the rest of its error body is withheld)",
        _ => "the rest of S3's error body is withheld",
    };
    match code {
        Some(code) => format!("<Error><Code>{code}</Code><Message>{message}</Message></Error>"),
        None => format!("<Error><Message>{message}</Message></Error>"),
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn an_error_body_is_read_up_to_the_cap_only() {
        use futures::StreamExt;
        let chunk = bytes::Bytes::from(vec![b'x'; 40 * 1024]);
        let polled = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = polled.clone();
        // A body that never ends: reading it unbounded would not return.
        let endless = futures::stream::repeat_with(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(chunk.clone())
        })
        .boxed();
        let read = super::read_capped(endless, 64 * 1024).await.unwrap();
        assert_eq!(read.len(), 64 * 1024);
        assert_eq!(polled.load(std::sync::atomic::Ordering::SeqCst), 2);
        // The code at the front survives the cut.
        let mut body = b"<Error><Code>SlowDown</Code>".to_vec();
        body.resize(200 * 1024, b'y');
        let one = futures::stream::iter([Ok(bytes::Bytes::from(body))]).boxed();
        let cut = super::read_capped(one, 64 * 1024).await.unwrap();
        assert!(
            super::redacted_error_body(503, &String::from_utf8_lossy(&cut))
                .contains("<Code>SlowDown</Code>")
        );
    }

    use super::*;

    /// An S3 authentication failure's body, as AWS and versitygw send it:
    /// it echoes the key id and the string to sign.
    const LEAKY_403: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we \
        calculated does not match the signature you provided.</Message>\
        <AWSAccessKeyId>AKIDLEAKCANARY</AWSAccessKeyId><StringToSign>AWS4-HMAC-SHA256&#xA;\
        20261003T000000Z&#xA;20261003/us-east-1/s3/aws4_request&#xA;CANARYSTRINGTOSIGN\
        </StringToSign><CanonicalRequest>GET&#xA;/b/meta.json</CanonicalRequest></Error>";

    /// Plan 37 K6a: no part of an S3 error body but its code reaches an
    /// error text — not through object_store's error, nor `StoreError`,
    /// nor an anyhow chain — and the failure still classifies by its code.
    #[tokio::test]
    async fn an_s3_error_body_never_reaches_an_error_message() {
        let (endpoint, _) = crate::scripted_http::scripted_s3_replies(vec![
            crate::scripted_http::Reply::with_body(403, LEAKY_403),
        ])
        .await;
        let s3 = configure_s3_client(
            AmazonS3Builder::new()
                .with_endpoint(&endpoint)
                .with_allow_http(true)
                .with_bucket_name("b")
                .with_region("us-east-1")
                .with_access_key_id("AKIDLEAKCANARY")
                .with_secret_access_key("s"),
        )
        .build()
        .unwrap();
        let store = crate::ChunkStore::new(std::sync::Arc::new(s3));
        let e = store.load_fs().await.unwrap_err();
        let classified = crate::classify(&e);
        let chained = format!("{:#}", anyhow::Error::new(e).context("reading meta.json"));
        for text in [&chained, &format!("{chained:?}")] {
            for leak in [
                "AKIDLEAKCANARY",
                "CANARYSTRINGTOSIGN",
                "StringToSign",
                "CanonicalRequest",
            ] {
                assert!(!text.contains(leak), "{leak} leaked: {text}");
            }
        }
        assert!(chained.contains("403"), "{chained}");
        assert!(chained.contains("SignatureDoesNotMatch"), "{chained}");
        assert!(chained.contains("S3 refused the credentials"), "{chained}");
        assert_eq!(classified, crate::ErrorClass::Permanent);
    }

    #[test]
    fn a_redacted_body_keeps_only_a_plain_code() {
        assert_eq!(
            redacted_error_body(
                404,
                "<Error><Code>NoSuchKey</Code><Key>secret/path</Key></Error>"
            ),
            "<Error><Code>NoSuchKey</Code><Message>the rest of S3's error body is withheld\
             </Message></Error>"
        );
        // A code that is not one (markup, spaces) is dropped, not echoed.
        let odd = redacted_error_body(403, "<Code>AKID <x>y</x></Code>");
        assert!(!odd.contains("AKID"), "{odd}");
        assert!(redacted_error_body(500, "<html>proxy</html>").starts_with("<Error><Message>"));
    }

    #[test]
    fn provider_name_is_read_without_the_keys() {
        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "secret-example",
            None,
            None,
            "ProfileFile",
        );
        assert_eq!(provider_name(&creds).as_deref(), Some("ProfileFile"));
    }

    #[test]
    fn resolution_names_the_default_endpoint_and_the_credentials() {
        let r = S3Resolution {
            endpoint: "https://s3.us-west-2.amazonaws.com".into(),
            endpoint_configured: false,
            region: "us-west-2".into(),
            credentials: Some("IMDSv2".into()),
            profile: None,
        };
        assert_eq!(
            r.to_string(),
            "endpoint https://s3.us-west-2.amazonaws.com (AWS default: no endpoint configured), \
             region us-west-2, credentials from IMDSv2, AWS_PROFILE unset"
        );
    }
}
