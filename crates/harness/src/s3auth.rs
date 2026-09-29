//! Harness-side S3 requests that work against either backend.
//!
//! The scenarios poke the bucket directly (list a prefix, HEAD a chunk,
//! plant an orphan object, count commits...) with plain `ureq` calls. floci
//! (the docker backend) accepts anonymous requests; versitygw (the process
//! backend) does not, so those calls need a SigV4 `Authorization` header
//! there. [`get`]/[`head`]/[`put`]/[`delete`] return the `ureq::Request` a
//! bare `ureq::get(url)` would, signed when (and only when) the process
//! backend is selected, so a call site changes by its function name and
//! nothing else, and the docker path stays byte-for-byte what it was.
//!
//! The signature uses `UNSIGNED-PAYLOAD`, which makes it independent of the
//! request body (the request can then be sent with `call()`, `send_bytes()`
//! or `send_json()` as before). Extra headers a caller adds afterwards
//! (`If-None-Match`, ...) are not part of `SignedHeaders`, which SigV4
//! permits. Credentials are the fixed pair every harness client uses
//! (`test`/`test`, region `us-east-1`; see `Client::cmd`).

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

pub const ACCESS_KEY: &str = "test";
pub const SECRET_KEY: &str = "test";
pub const REGION: &str = "us-east-1";

type HmacSha256 = Hmac<Sha256>;

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Percent-encode everything except the SigV4 unreserved set (and `/` when
/// `keep_slash`), after decoding any existing escapes so an already-encoded
/// input is not double-encoded.
fn encode(s: &str, keep_slash: bool) -> String {
    let raw = decode(s);
    let mut out = String::with_capacity(raw.len());
    for &b in &raw {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn decode(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Some(v) = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    out
}

/// `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` for a Unix timestamp (UTC).
fn amz_times(unix: u64) -> (String, String) {
    let days = (unix / 86_400) as i64;
    let rem = unix % 86_400;
    // Civil-from-days (Howard Hinnant), proleptic Gregorian.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let date = format!("{y:04}{m:02}{d:02}");
    let stamp = format!(
        "{date}T{:02}{:02}{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    );
    (date, stamp)
}

/// `(authority, path, query)` of an `http(s)://` URL.
fn split_url(url: &str) -> (&str, &str, &str) {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let (authority, path_query) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let path_query = path_query.split('#').next().unwrap_or("/");
    match path_query.split_once('?') {
        Some((p, q)) => (authority, p, q),
        None => (authority, path_query, ""),
    }
}

/// The three headers that authenticate `method url` at time `unix`:
/// `x-amz-date`, `x-amz-content-sha256` and `authorization`.
pub fn sign_at(method: &str, url: &str, unix: u64) -> [(&'static str, String); 3] {
    sign_with(
        method,
        url,
        unix,
        (ACCESS_KEY, SECRET_KEY, REGION),
        "UNSIGNED-PAYLOAD",
    )
}

/// [`sign_at`] with explicit `(access key, secret key, region)` and payload
/// hash (split out so the AWS documentation's test vectors can be checked).
fn sign_with(
    method: &str,
    url: &str,
    unix: u64,
    (access, secret, region): (&str, &str, &str),
    payload: &str,
) -> [(&'static str, String); 3] {
    let (authority, path, query) = split_url(url);
    let (date, stamp) = amz_times(unix);
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (encode(k, false), encode(v, false))
        })
        .collect();
    pairs.sort();
    let canonical_query = pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical = format!(
        "{method}\n{}\n{canonical_query}\nhost:{authority}\nx-amz-content-sha256:{payload}\n\
         x-amz-date:{stamp}\n\n{signed_headers}\n{payload}",
        encode(path, true)
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let k = hmac(format!("AWS4{secret}").as_bytes(), &date);
    let k = hmac(&k, region);
    let k = hmac(&k, "s3");
    let k = hmac(&k, "aws4_request");
    let signature = hex(&hmac(&k, &to_sign));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed_headers}, \
         Signature={signature}"
    );
    [
        ("x-amz-date", stamp),
        ("x-amz-content-sha256", payload.to_string()),
        ("authorization", authorization),
    ]
}

/// A request for `method url`, always signed.
pub fn signed(method: &str, url: &str) -> ureq::Request {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    sign_at(method, url, now)
        .into_iter()
        .fold(ureq::request(method, url), |r, (k, v)| r.set(k, &v))
}

/// A request for `method url`, signed if the selected S3 backend needs it.
pub fn request(method: &str, url: &str) -> ureq::Request {
    if crate::s3env::backend().needs_auth() {
        signed(method, url)
    } else {
        ureq::request(method, url)
    }
}

pub fn get(url: &str) -> ureq::Request {
    request("GET", url)
}

pub fn head(url: &str) -> ureq::Request {
    request("HEAD", url)
}

pub fn put(url: &str) -> ureq::Request {
    request("PUT", url)
}

pub fn delete(url: &str) -> ureq::Request {
    request("DELETE", url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times() {
        assert_eq!(amz_times(0), ("19700101".into(), "19700101T000000Z".into()));
        // 2013-05-24T00:00:00Z, the AWS SigV4 documentation's example date.
        assert_eq!(
            amz_times(1_369_353_600),
            ("20130524".into(), "20130524T000000Z".into())
        );
        assert_eq!(
            amz_times(1_709_210_096),
            ("20240229".into(), "20240229T123456Z".into())
        );
    }

    #[test]
    fn encoding() {
        assert_eq!(encode("a b/c", true), "a%20b/c");
        assert_eq!(encode("a b/c", false), "a%20b%2Fc");
        assert_eq!(encode("a%20b", false), "a%20b");
        assert_eq!(encode("x+y", false), "x%2By");
        assert_eq!(encode("~-._", false), "~-._");
    }

    #[test]
    fn url_split() {
        assert_eq!(
            split_url("http://127.0.0.1:9/b/k?list-type=2&prefix=a/b"),
            ("127.0.0.1:9", "/b/k", "list-type=2&prefix=a/b")
        );
        assert_eq!(split_url("http://h:1"), ("h:1", "/", ""));
    }

    /// The shape of what the harness actually sends (`UNSIGNED-PAYLOAD`, our
    /// credentials); correctness is pinned by [`aws_documentation_vectors`].
    #[test]
    fn signature_shape_is_stable() {
        let h = sign_at(
            "GET",
            "http://127.0.0.1:9000/b?list-type=2&prefix=x/y",
            1_369_353_600,
        );
        assert_eq!(h[0], ("x-amz-date", "20130524T000000Z".to_string()));
        assert_eq!(h[1].1, "UNSIGNED-PAYLOAD");
        let auth = &h[2].1;
        assert!(auth.starts_with(
            "AWS4-HMAC-SHA256 Credential=test/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="
        ));
        let sig = auth.rsplit('=').next().unwrap();
        assert_eq!(sig.len(), 64);
        assert!(sig.bytes().all(|b| b.is_ascii_hexdigit()));
        // Same inputs, same signature; a different query, a different one.
        assert_eq!(
            h,
            sign_at(
                "GET",
                "http://127.0.0.1:9000/b?prefix=x/y&list-type=2",
                1_369_353_600
            )
        );
        assert_ne!(
            h[2].1,
            sign_at("GET", "http://127.0.0.1:9000/b?prefix=x", 1_369_353_600)[2].1
        );
    }

    /// Known-answer tests: the AWS S3 SigV4 documentation's header-signing
    /// examples ("Authenticating Requests: Using the Authorization Header",
    /// `examplebucket`, 2013-05-24, `AKIAIOSFODNN7EXAMPLE`), whose signed
    /// headers are exactly ours. They pin the canonical request (query
    /// sorting and encoding, a value-less parameter, header canonicalisation),
    /// the credential scope and the key derivation end to end.
    #[test]
    fn aws_documentation_vectors() {
        const EMPTY_SHA256: &str =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let creds = (
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
        );
        let sig = |url: &str| {
            let h = sign_with("GET", url, 1_369_353_600, creds, EMPTY_SHA256);
            assert_eq!(h[0].1, "20130524T000000Z");
            assert_eq!(h[1].1, EMPTY_SHA256);
            assert!(h[2].1.starts_with(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/\
                 aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="
            ));
            h[2].1.rsplit('=').next().unwrap().to_string()
        };
        // GET Bucket lifecycle: a value-less query parameter.
        assert_eq!(
            sig("https://examplebucket.s3.amazonaws.com/?lifecycle"),
            "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        );
        // GET Bucket (List Objects): query parameters sorted by name.
        assert_eq!(
            sig("https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J"),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
        assert_eq!(
            sig("https://examplebucket.s3.amazonaws.com/?prefix=J&max-keys=2"),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    /// Known-answer test: the AWS SigV4 signing-key derivation example
    /// (secret `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`, 20120215,
    /// us-east-1, iam) from the AWS documentation.
    #[test]
    fn signing_key_known_answer() {
        let k = hmac(b"AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20120215");
        let k = hmac(&k, "us-east-1");
        let k = hmac(&k, "iam");
        let k = hmac(&k, "aws4_request");
        assert_eq!(
            hex(&k),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }
}
