//! A counting HTTP relay that sits in front of the S3 path and records
//! every request the constellation clients make, by method and target.
//!
//! Plan 26's whole argument is about *request classes*: replacing an idle
//! `LIST` with a speculative `GET`, and then backing the poll off so there
//! are far fewer of either. Nothing else in the harness can see that.
//! toxiproxy is a TCP fault injector with no notion of HTTP, and floci
//! logs bucket lifecycle only — neither can answer "how many LISTs of
//! `log/` did an idle cluster issue". A scenario asserting "zero LISTs of
//! `log/p0` during the burst" needs the actual wire.
//!
//! So this is a plain TCP relay that additionally parses the
//! client→upstream direction as HTTP/1.1 and appends one [`Request`] per
//! request line. Bytes are forwarded verbatim in both directions and the
//! relay never rewrites, buffers whole bodies, or answers anything itself;
//! parsing is a side effect, so a parse that loses framing cannot corrupt
//! the S3 conversation — it can only miscount. To keep a miscount from
//! passing as a result, losing framing sets [`CountingProxy::desyncs`],
//! which every scenario asserts is zero.
//!
//! It is chained *in front of* toxiproxy (client → counter → toxiproxy →
//! floci) so latency and cut toxics still apply to the same connections
//! being counted.
//!
//! Plan 30 M0 adds a second, unrelated capability: `cut()`/`heal()` turn
//! this relay into a per-node S3 kill switch. Unlike `Toxiproxy::Proxy`'s
//! `cut`/`heal` (shared by every client using that proxy, since it is one
//! toxiproxy route), a `CountingProxy` is created per client
//! (`S3Env::counting_proxy`), so cutting it takes exactly one node's S3
//! path away — needed to reproduce bug B (a holder stranded mid-write
//! while its peers keep working).

use anyhow::{Context, Result};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One observed HTTP request: the method and the raw request target
/// (`/bucket/key?query`).
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub target: String,
}

impl Request {
    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or(&self.target)
    }

    pub fn query(&self) -> &str {
        self.target.split_once('?').map(|(_, q)| q).unwrap_or("")
    }

    /// S3 ListObjectsV2: a `GET` on the bucket with `list-type=2`.
    pub fn is_list(&self) -> bool {
        self.method == "GET" && self.query().contains("list-type=")
    }

    /// A `LIST` whose `prefix=` names `needle` (URL-encoded `/` included,
    /// since object_store percent-encodes the prefix).
    pub fn lists_prefix(&self, needle: &str) -> bool {
        if !self.is_list() {
            return false;
        }
        let decoded = percent_decode(self.query());
        decoded.contains(needle)
    }

    /// Bucket/key requests whose key sits under `needle`.
    pub fn touches(&self, needle: &str) -> bool {
        percent_decode(self.path()).contains(needle) || self.lists_prefix(needle)
    }

    /// The bucket area this request touches — `log`, `chunks`,
    /// `commits`, `packs`, `nodes`, `leases`, `designations`, … Every scenario
    /// puts its filesystem under a per-run key prefix, so the area is the
    /// segment after the bucket and that prefix; a LIST names it in
    /// `prefix=` instead of in the path.
    ///
    /// This is what turns a bare class tally into an answerable question:
    /// "30 LISTs" is only meaningful once you can see they are all
    /// membership polls and none of them is `log/`.
    pub fn area(&self) -> String {
        let is_list = self.is_list();
        let raw = if is_list {
            percent_decode(self.query())
                .split('&')
                .find_map(|kv| kv.strip_prefix("prefix=").map(str::to_string))
                .unwrap_or_default()
        } else {
            percent_decode(self.path())
        };
        let mut segments = raw.split('/').filter(|s| !s.is_empty());
        if !is_list {
            segments.next(); // bucket
        }
        segments.next(); // per-run filesystem prefix
        segments.next().unwrap_or("(root)").to_string()
    }
}

/// Minimal percent-decoding: enough to turn `prefix=p%2Flog%2Fp0%2F` back
/// into something a substring test can read.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Requests grouped the way S3 prices them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    pub list: u64,
    pub get: u64,
    pub head: u64,
    pub put: u64,
    pub post: u64,
    pub delete: u64,
    pub other: u64,
}

impl Tally {
    pub fn total(&self) -> u64 {
        self.list + self.get + self.head + self.put + self.post + self.delete + self.other
    }
}

impl std::fmt::Display for Tally {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LIST={} GET={} HEAD={} PUT={} POST={} DELETE={} other={} total={}",
            self.list,
            self.get,
            self.head,
            self.put,
            self.post,
            self.delete,
            self.other,
            self.total()
        )
    }
}

/// Requests grouped by class *and* bucket area, busiest first. The class
/// tally says what a node spent; this says what it spent it on.
pub fn breakdown(requests: &[Request]) -> String {
    let mut counts: std::collections::BTreeMap<(String, String), u64> = Default::default();
    for r in requests {
        let class = if r.is_list() {
            "LIST".to_string()
        } else {
            r.method.clone()
        };
        *counts.entry((class, r.area())).or_default() += 1;
    }
    let mut rows: Vec<_> = counts.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows.iter()
        .map(|((class, area), n)| format!("{class} {area}={n}"))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn tally(requests: &[Request]) -> Tally {
    let mut t = Tally::default();
    for r in requests {
        match r.method.as_str() {
            _ if r.is_list() => t.list += 1,
            "GET" => t.get += 1,
            "HEAD" => t.head += 1,
            "PUT" | "PATCH" => t.put += 1,
            "POST" => t.post += 1,
            "DELETE" => t.delete += 1,
            _ => t.other += 1,
        }
    }
    t
}

/// A counting relay in front of an upstream `host:port`.
pub struct CountingProxy {
    port: u16,
    log: Arc<Mutex<Vec<Request>>>,
    desyncs: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    /// Plan 30 M0's per-node S3 switch: see [`Self::cut`].
    cut: Arc<AtomicBool>,
}

impl CountingProxy {
    /// Listen on an ephemeral loopback port, forwarding to `upstream`
    /// (`host:port`).
    pub fn start(upstream: &str) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").context("binding the counting proxy")?;
        let port = listener.local_addr()?.port();
        listener
            .set_nonblocking(true)
            .context("counting proxy nonblocking accept")?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let desyncs = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let cut = Arc::new(AtomicBool::new(false));
        let upstream = upstream.to_string();
        {
            let (log, desyncs, stop, cut) =
                (log.clone(), desyncs.clone(), stop.clone(), cut.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((client, _)) => {
                            // While cut, a client retrying its connection
                            // must see every attempt fail, not just watch
                            // its existing connections drop — otherwise a
                            // fresh connect would quietly restore service
                            // through the accept loop alone.
                            if cut.load(Ordering::Relaxed) {
                                let _ = client.shutdown(std::net::Shutdown::Both);
                                continue;
                            }
                            let (log, desyncs, stop, upstream, cut) = (
                                log.clone(),
                                desyncs.clone(),
                                stop.clone(),
                                upstream.clone(),
                                cut.clone(),
                            );
                            std::thread::spawn(move || {
                                let _ = relay(client, &upstream, log, desyncs, stop, cut);
                            });
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });
        }
        Ok(Self {
            port,
            log,
            desyncs,
            stop,
            cut,
        })
    }

    /// The endpoint constellation clients should use.
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Cut this proxy's S3 path: every connection currently relaying is
    /// closed within one poll tick (~100 ms — the relay loops' existing
    /// read-timeout granularity), and newly accepted connections are
    /// refused immediately. Counting is unaffected. Pairs with
    /// [`Self::heal`].
    pub fn cut(&self) {
        self.cut.store(true, Ordering::Relaxed);
    }

    /// Undo [`Self::cut`]: new connections relay normally again.
    /// Connections closed while cut are not reopened — the S3 client
    /// reconnects on its own retry, exactly as it would after a real
    /// outage.
    pub fn heal(&self) {
        self.cut.store(false, Ordering::Relaxed);
    }

    pub fn requests(&self) -> Vec<Request> {
        self.log.lock().unwrap().clone()
    }

    /// Forget everything recorded so far; the next `requests()` covers
    /// only what happened after this call.
    pub fn reset(&self) {
        self.log.lock().unwrap().clear();
        self.desyncs.store(0, Ordering::Relaxed);
    }

    pub fn tally(&self) -> Tally {
        tally(&self.requests())
    }

    /// Number of connections whose HTTP framing could not be followed. A
    /// non-zero value invalidates every count, so scenarios must check it.
    pub fn desyncs(&self) -> u64 {
        self.desyncs.load(Ordering::Relaxed)
    }

    /// Fail if framing was lost anywhere — the counts would be fiction.
    pub fn ensure_sane(&self) -> Result<()> {
        let n = self.desyncs();
        anyhow::ensure!(
            n == 0,
            "the request counter lost HTTP framing on {n} connection(s); \
             its tallies cannot be trusted"
        );
        Ok(())
    }
}

impl Drop for CountingProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Pump one client connection to `upstream` and back, parsing the
/// client→upstream direction for request lines.
fn relay(
    client: TcpStream,
    upstream: &str,
    log: Arc<Mutex<Vec<Request>>>,
    desyncs: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    cut: Arc<AtomicBool>,
) -> Result<()> {
    client.set_nodelay(true).ok();
    let server = TcpStream::connect(upstream).context("counting proxy upstream connect")?;
    server.set_nodelay(true).ok();
    let poll = Duration::from_millis(100);
    client.set_read_timeout(Some(poll)).ok();
    server.set_read_timeout(Some(poll)).ok();

    // upstream -> client: a plain copy, nothing to parse.
    let back = {
        let (mut from, mut to) = (server.try_clone()?, client.try_clone()?);
        let (stop, cut) = (stop.clone(), cut.clone());
        std::thread::spawn(move || {
            let mut buf = [0u8; 64 << 10];
            loop {
                // Checked every iteration, so a connection idling inside
                // the blocking `read` below (bounded by the 100ms timeout
                // set above) still notices a cut within one poll tick.
                if cut.load(Ordering::Relaxed) {
                    let _ = from.shutdown(std::net::Shutdown::Both);
                    let _ = to.shutdown(std::net::Shutdown::Both);
                    break;
                }
                match from.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if to.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    Err(ref e) if would_block(e) => {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = to.shutdown(std::net::Shutdown::Write);
        })
    };

    let (mut from, mut to) = (client, server.try_clone()?);
    let mut parser = RequestParser::default();
    let mut buf = [0u8; 64 << 10];
    loop {
        if cut.load(Ordering::Relaxed) {
            let _ = from.shutdown(std::net::Shutdown::Both);
            let _ = to.shutdown(std::net::Shutdown::Both);
            break;
        }
        match from.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    break;
                }
                if let Some(found) = parser.feed(&buf[..n]) {
                    log.lock().unwrap().extend(found);
                } else {
                    desyncs.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            Err(ref e) if would_block(e) => {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    let _ = to.shutdown(std::net::Shutdown::Write);
    let _ = back.join();
    Ok(())
}

fn would_block(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

/// Incremental HTTP/1.1 request-stream parser: heads are recorded, bodies
/// are skipped by `Content-Length` or chunk framing.
#[derive(Default)]
struct RequestParser {
    buf: Vec<u8>,
    state: ParseState,
}

#[derive(Default)]
enum ParseState {
    #[default]
    Head,
    /// Remaining body bytes to skip.
    Body(usize),
    /// Awaiting a chunk-size line.
    ChunkHead,
    /// Remaining bytes of the current chunk, including its trailing CRLF.
    ChunkBody(usize),
    /// After the last chunk: trailers (if any) then a bare CRLF.
    ChunkEnd,
}

impl RequestParser {
    /// Consume `bytes`; `None` means framing was lost and every later
    /// count on this connection would be guesswork.
    fn feed(&mut self, bytes: &[u8]) -> Option<Vec<Request>> {
        self.buf.extend_from_slice(bytes);
        let mut found = Vec::new();
        loop {
            match self.state {
                ParseState::Head => {
                    let Some(end) = find(&self.buf, b"\r\n\r\n") else {
                        return Some(found);
                    };
                    let head = String::from_utf8_lossy(&self.buf[..end]).to_string();
                    self.buf.drain(..end + 4);
                    let mut lines = head.lines();
                    let request_line = lines.next().unwrap_or_default();
                    let mut parts = request_line.split(' ');
                    let (method, target, version) =
                        (parts.next()?, parts.next()?, parts.next().unwrap_or(""));
                    if !version.starts_with("HTTP/")
                        || !method.chars().all(|c| c.is_ascii_uppercase())
                    {
                        return None;
                    }
                    found.push(Request {
                        method: method.to_string(),
                        target: target.to_string(),
                    });
                    let mut length = 0usize;
                    let mut chunked = false;
                    for line in lines {
                        let Some((name, value)) = line.split_once(':') else {
                            continue;
                        };
                        let (name, value) = (name.trim().to_ascii_lowercase(), value.trim());
                        if name == "content-length" {
                            length = value.parse().ok()?;
                        } else if name == "transfer-encoding"
                            && value.to_ascii_lowercase().contains("chunked")
                        {
                            chunked = true;
                        }
                    }
                    self.state = if chunked {
                        ParseState::ChunkHead
                    } else if length > 0 {
                        ParseState::Body(length)
                    } else {
                        ParseState::Head
                    };
                }
                ParseState::Body(remaining) => {
                    let take = remaining.min(self.buf.len());
                    self.buf.drain(..take);
                    self.state = match remaining - take {
                        0 => ParseState::Head,
                        left => ParseState::Body(left),
                    };
                    if take == 0 {
                        return Some(found);
                    }
                }
                ParseState::ChunkHead => {
                    let Some(end) = find(&self.buf, b"\r\n") else {
                        return Some(found);
                    };
                    let line = String::from_utf8_lossy(&self.buf[..end]).to_string();
                    self.buf.drain(..end + 2);
                    let size_hex = line.split(';').next().unwrap_or("").trim();
                    let size = usize::from_str_radix(size_hex, 16).ok()?;
                    self.state = if size == 0 {
                        ParseState::ChunkEnd
                    } else {
                        ParseState::ChunkBody(size + 2)
                    };
                }
                ParseState::ChunkBody(remaining) => {
                    let take = remaining.min(self.buf.len());
                    self.buf.drain(..take);
                    self.state = match remaining - take {
                        0 => ParseState::ChunkHead,
                        left => ParseState::ChunkBody(left),
                    };
                    if take == 0 {
                        return Some(found);
                    }
                }
                ParseState::ChunkEnd => {
                    // Either a bare CRLF (no trailers) or trailer lines
                    // terminated by one.
                    if self.buf.starts_with(b"\r\n") {
                        self.buf.drain(..2);
                        self.state = ParseState::Head;
                    } else {
                        let Some(end) = find(&self.buf, b"\r\n\r\n") else {
                            return Some(found);
                        };
                        self.buf.drain(..end + 4);
                        self.state = ParseState::Head;
                    }
                }
            }
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed_all(chunks: &[&[u8]]) -> Vec<Request> {
        let mut parser = RequestParser::default();
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend(parser.feed(chunk).expect("framing kept"));
        }
        out
    }

    #[test]
    fn keep_alive_requests_are_counted_across_arbitrary_tcp_splits() {
        let stream = concat!(
            "GET /b?list-type=2&prefix=p%2Flog%2Fp0%2F HTTP/1.1\r\nHost: x\r\n\r\n",
            "PUT /b/p/log/p0/000001 HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello",
            "HEAD /b/p/chunks/aa HTTP/1.1\r\nHost: x\r\n\r\n",
        )
        .as_bytes();
        // A body containing something that looks like a request line must
        // not be miscounted, so split at every possible boundary.
        for split in 1..stream.len() {
            let seen = feed_all(&[&stream[..split], &stream[split..]]);
            let methods: Vec<&str> = seen.iter().map(|r| r.method.as_str()).collect();
            assert_eq!(methods, ["GET", "PUT", "HEAD"], "split at {split}");
            assert!(seen[0].is_list());
            assert!(seen[0].lists_prefix("/log/p0/"));
            assert!(!seen[1].is_list());
            assert!(seen[1].touches("/log/p0/"));
            assert!(!seen[2].touches("/log/"));
        }
    }

    #[test]
    fn a_body_that_looks_like_a_request_is_not_counted() {
        let stream =
            b"PUT /b/k HTTP/1.1\r\nContent-Length: 38\r\n\r\nGET /b?list-type=2 HTTP/1.1\r\nHost: x\r\n\r\n";
        let seen = feed_all(&[stream]);
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "PUT");
    }

    #[test]
    fn chunked_bodies_are_skipped() {
        let stream = b"POST /b/k?uploads HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nabcd\r\n0\r\n\r\nGET /b/k HTTP/1.1\r\n\r\n";
        let seen = feed_all(&[stream]);
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(seen[1].method, "GET");
    }

    #[test]
    fn garbage_reports_lost_framing_instead_of_guessing() {
        let mut parser = RequestParser::default();
        assert!(parser
            .feed(b"\x00\x01\x02 not http at all\r\n\r\n")
            .is_none());
    }

    #[test]
    fn tally_splits_lists_out_of_gets() {
        let seen = feed_all(&[concat!(
            "GET /b?list-type=2&prefix=x HTTP/1.1\r\n\r\n",
            "GET /b/k HTTP/1.1\r\n\r\n",
            "DELETE /b/k HTTP/1.1\r\n\r\n",
        )
        .as_bytes()]);
        let t = tally(&seen);
        assert_eq!(
            t,
            Tally {
                list: 1,
                get: 1,
                delete: 1,
                ..Tally::default()
            }
        );
        assert_eq!(t.total(), 3);
    }
}
