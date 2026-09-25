//! Test support: a scripted HTTP server that answers `object_store`'s real
//! S3 client, for reproducing a backend's observed replies (OVH's 409
//! before 412; a bucket that is not there) without the backend.

use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// One scripted reply: a status and its body.
#[derive(Debug, Clone)]
pub(crate) struct Reply {
    pub status: u16,
    pub body: String,
}

impl Reply {
    /// The body S3 sends with `status` when a test does not care.
    pub(crate) fn status(status: u16) -> Self {
        let body = match status {
            409 => "<Error><Code>OperationAborted</Code><Message>A conflicting \
                    conditional operation is currently in progress against this \
                    resource.</Message></Error>"
                .to_string(),
            412 => "<Error><Code>PreconditionFailed</Code></Error>".to_string(),
            _ => String::new(),
        };
        Self { status, body }
    }

    pub(crate) fn with_body(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

/// What the server saw of one request.
pub(crate) type Seen = Arc<std::sync::Mutex<Vec<String>>>;

/// [`scripted_s3_replies`] with S3's usual bodies for `statuses`.
pub(crate) async fn scripted_s3(statuses: Vec<u16>) -> (String, Seen) {
    scripted_s3_replies(statuses.into_iter().map(Reply::status).collect()).await
}

/// A one-shot HTTP responder: answers the n-th request with `replies[n]`
/// (the last one repeats), recording each request's method and path
/// (`"GET /b/meta.json"`; [`methods`] keeps just the methods). Enough of
/// HTTP/1.1 for `object_store`'s S3 client.
pub(crate) async fn scripted_s3_replies(replies: Vec<Reply>) -> (String, Seen) {
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
            let reply = replies[n.min(replies.len() - 1)].clone();
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
                let request_line = head.lines().next().unwrap_or("");
                let mut parts = request_line.split(' ');
                let method = parts.next().unwrap_or("");
                let target = parts.next().unwrap_or("");
                log.lock().unwrap().push(format!("{method} {target}"));
                let reason = match reply.status {
                    200 => "OK",
                    404 => "Not Found",
                    409 => "Conflict",
                    412 => "Precondition Failed",
                    _ => "Error",
                };
                // A HEAD answer carries no body.
                let body = if method == "HEAD" { "" } else { &reply.body };
                let answer = format!(
                    "HTTP/1.1 {} {reason}\r\nContent-Length: {}\r\nETag: \"e1\"\r\n\
                     Connection: close\r\n\r\n{body}",
                    reply.status,
                    body.len()
                );
                let _ = sock.write_all(answer.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

/// The methods of the requests `seen` recorded, in order.
pub(crate) fn methods(seen: &Seen) -> Vec<String> {
    seen.lock()
        .unwrap()
        .iter()
        .map(|r| r.split(' ').next().unwrap_or("").to_string())
        .collect()
}

/// `object_store`'s real S3 client against `endpoint`, bucket `b`, no
/// retries.
pub(crate) fn real_s3(endpoint: &str) -> Arc<dyn ObjectStore> {
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
