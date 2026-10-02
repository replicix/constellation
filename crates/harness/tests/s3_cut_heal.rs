//! The daemons' own S3 client (`configure_s3_client`: object_store's S3
//! client with the daemons' retry policy) through the harness's counting
//! relay (`CountingProxy`), across a cut and a heal in the middle of a
//! 15-way burst of GETs — the shape of `epoch-member-dies-with-chunk`,
//! where a member's chunk-fetch burst met its S3 cut.
//!
//! Two things are held here at once, because a failure of either looks
//! the same from a scenario ("every request fails after the heal"):
//! - the relay models a cut and a heal faithfully: while cut, nothing
//!   reaches the upstream and every connection attempt fails; once
//!   healed, new connections relay again and every request is counted;
//! - the client recovers as soon as S3 is reachable: no connection the
//!   cut killed is reused, no backoff or breaker state carries over, and
//!   the next burst succeeds at once, at full width.
//!
//! The upstream is a tiny keep-alive HTTP/1.1 server (so the client's
//! pool holds idle connections when the cut comes) that answers every
//! GET with a fixed body, held back while asked to (so a burst is in
//! flight when the cut comes: the tests wait until the upstream has
//! every request of it, cut, and only then let the answers go — a timed
//! delay raced the cut on a loaded host, where the test's sleep before
//! the cut could outlast the delay and the whole burst completed).

use constellation_harness::reqlog::CountingProxy;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BODY: usize = 64 << 10;
const BURST: usize = 15;

/// A keep-alive S3 stand-in: every request is answered 200 with `BODY`
/// bytes, once `hold` is off.
struct Upstream {
    addr: String,
    hold: Arc<AtomicBool>,
    /// Requests read (answered or held): what reached S3. Counted
    /// before the answer, not after it: a count bumped once the last
    /// write returned can lag a client that has read the whole answer
    /// already (the oracle used to count answers, and `served >= served
    /// + BURST` failed under load in a gate run).
    received: Arc<AtomicU64>,
}

fn upstream() -> Upstream {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let hold = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicU64::new(0));
    {
        let (hold, received) = (hold.clone(), received.clone());
        std::thread::spawn(move || {
            for sock in listener.incoming() {
                let Ok(sock) = sock else { return };
                let (hold, received) = (hold.clone(), received.clone());
                std::thread::spawn(move || serve(sock, &hold, &received));
            }
        });
    }
    Upstream {
        addr,
        hold,
        received,
    }
}

fn serve(mut sock: TcpStream, hold: &AtomicBool, received: &AtomicU64) {
    let body = vec![7u8; BODY];
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
            match sock.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(k) => buf.extend_from_slice(&chunk[..k]),
            }
        };
        buf.drain(..end);
        received.fetch_add(1, Ordering::Relaxed);
        while hold.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(5));
        }
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {BODY}\r\nETag: \"e1\"\r\n\
             Last-Modified: Tue, 15 Nov 1994 08:12:31 GMT\r\n\r\n"
        );
        if sock.write_all(head.as_bytes()).is_err() || sock.write_all(&body).is_err() {
            return;
        }
    }
}

fn client(endpoint: &str) -> Arc<dyn ObjectStore> {
    let builder = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_bucket_name("b")
        .with_region("us-east-1")
        .with_access_key_id("k")
        .with_secret_access_key("s");
    // The harness's daemons run with this retry budget
    // (`Client::new`: 2 retries within 2 s); the rest is the product's.
    let s3 = constellation_store_s3::configure_s3_client(builder)
        .with_retry(object_store::RetryConfig {
            max_retries: 2,
            retry_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .build()
        .unwrap();
    Arc::new(s3)
}

/// `BURST` concurrent GETs; how many succeeded.
async fn burst(s3: &Arc<dyn ObjectStore>, tag: &str) -> usize {
    let tasks: Vec<_> = (0..BURST)
        .map(|i| {
            let s3 = s3.clone();
            let key = Path::from(format!("chunks/{tag}/{i}"));
            tokio::spawn(async move {
                match s3.get(&key).await {
                    Ok(r) => r.bytes().await.map(|b| b.len() == BODY).unwrap_or(false),
                    Err(_) => false,
                }
            })
        })
        .collect();
    let mut ok = 0;
    for t in tasks {
        if t.await.unwrap() {
            ok += 1;
        }
    }
    ok
}

/// A burst whose every GET has reached the upstream and is held there:
/// whatever happens to the path next happens with all of it in flight.
async fn held_burst(s3: &Arc<dyn ObjectStore>, up: &Upstream) -> tokio::task::JoinHandle<usize> {
    let received = up.received.load(Ordering::Relaxed);
    up.hold.store(true, Ordering::Relaxed);
    let in_flight = {
        let s3 = s3.clone();
        tokio::spawn(async move { burst(&s3, "in-flight").await })
    };
    let t0 = Instant::now();
    while up.received.load(Ordering::Relaxed) < received + BURST as u64 {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "the burst never reached the upstream ({} of {BURST})",
            up.received.load(Ordering::Relaxed) - received
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    in_flight
}

/// After the heal, the next burst succeeds in full and promptly; returns
/// how long it took.
async fn recovers(s3: &Arc<dyn ObjectStore>, proxy: &CountingProxy, up: &Upstream) -> Duration {
    let (counted, received) = (proxy.requests().len(), up.received.load(Ordering::Relaxed));
    let t0 = Instant::now();
    let ok = burst(s3, "after-heal").await;
    let took = t0.elapsed();
    assert_eq!(ok, BURST, "only {ok}/{BURST} GETs succeeded after the heal");
    assert!(
        took < Duration::from_secs(5),
        "the first burst after the heal took {took:?}"
    );
    assert!(
        proxy.requests().len() >= counted + BURST,
        "the relay did not count the requests after the heal"
    );
    assert!(up.received.load(Ordering::Relaxed) >= received + BURST as u64);
    // And the one after that, on the pool the first one refilled.
    assert_eq!(burst(s3, "after-heal-2").await, BURST);
    proxy.ensure_sane().unwrap();
    took
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_in_the_middle_of_a_burst_heals_at_once() {
    let up = upstream();
    let proxy = CountingProxy::start(&up.addr).unwrap();
    let s3 = client(&proxy.endpoint());

    // A warm pool: `BURST` idle keep-alive connections.
    assert_eq!(burst(&s3, "warm").await, BURST);

    // The cut comes with a burst in flight; its answers are let go once
    // the cut is in place.
    let in_flight = held_burst(&s3, &up).await;
    proxy.cut();
    let received_at_cut = up.received.load(Ordering::Relaxed);
    up.hold.store(false, Ordering::Relaxed);
    let ok = in_flight.await.unwrap();
    assert!(ok < BURST, "the cut killed none of the burst ({ok} ok)");

    // While cut, every request fails fast and nothing reaches upstream.
    let counted = proxy.requests().len();
    let t0 = Instant::now();
    assert_eq!(
        burst(&s3, "while-cut").await,
        0,
        "a GET went through the cut"
    );
    assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
    assert_eq!(proxy.requests().len(), counted, "the cut relayed a request");
    // (The held answers may complete upstream; no request may reach it:
    // neither the in-flight burst's retries nor the burst made while cut.)
    assert_eq!(
        up.received.load(Ordering::Relaxed),
        received_at_cut,
        "a request reached the upstream through the cut"
    );

    proxy.heal();
    let took = recovers(&s3, &proxy, &up).await;
    eprintln!("cut: first burst after the heal took {took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_black_hole_in_the_middle_of_a_burst_heals_at_once() {
    let up = upstream();
    let proxy = CountingProxy::start(&up.addr).unwrap();
    let s3 = client(&proxy.endpoint());
    assert_eq!(burst(&s3, "warm").await, BURST);

    // A firewall DROP with a burst in flight: nothing is answered (the
    // upstream's answers, let go once the hole is in place, are
    // swallowed).
    let in_flight = held_burst(&s3, &up).await;
    proxy.blackhole();
    up.hold.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!in_flight.is_finished(), "a black-holed burst returned");

    // The heal closes what sat in the hole: the stalled requests fail or
    // retry through, promptly either way.
    proxy.heal();
    let t0 = Instant::now();
    let _ = in_flight.await.unwrap();
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "the black-holed burst took {:?} to return after the heal",
        t0.elapsed()
    );
    let took = recovers(&s3, &proxy, &up).await;
    eprintln!("black hole: first burst after the heal took {took:?}");
}
