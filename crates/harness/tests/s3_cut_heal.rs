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
//! GET with a fixed body, after an optional delay (so a burst is in
//! flight when the cut comes).

use constellation_harness::reqlog::CountingProxy;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BODY: usize = 64 << 10;
const BURST: usize = 15;

/// A keep-alive S3 stand-in: every request is answered 200 with `BODY`
/// bytes, after `delay_ms`.
struct Upstream {
    addr: String,
    delay_ms: Arc<AtomicU64>,
    served: Arc<AtomicU64>,
}

fn upstream() -> Upstream {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let delay_ms = Arc::new(AtomicU64::new(0));
    let served = Arc::new(AtomicU64::new(0));
    {
        let (delay_ms, served) = (delay_ms.clone(), served.clone());
        std::thread::spawn(move || {
            for sock in listener.incoming() {
                let Ok(sock) = sock else { return };
                let (delay_ms, served) = (delay_ms.clone(), served.clone());
                std::thread::spawn(move || serve(sock, &delay_ms, &served));
            }
        });
    }
    Upstream {
        addr,
        delay_ms,
        served,
    }
}

fn serve(mut sock: TcpStream, delay_ms: &AtomicU64, served: &AtomicU64) {
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
        std::thread::sleep(Duration::from_millis(delay_ms.load(Ordering::Relaxed)));
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {BODY}\r\nETag: \"e1\"\r\n\
             Last-Modified: Tue, 15 Nov 1994 08:12:31 GMT\r\n\r\n"
        );
        if sock.write_all(head.as_bytes()).is_err() || sock.write_all(&body).is_err() {
            return;
        }
        served.fetch_add(1, Ordering::Relaxed);
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

/// After the heal, the next burst succeeds in full and promptly; returns
/// how long it took.
async fn recovers(s3: &Arc<dyn ObjectStore>, proxy: &CountingProxy, up: &Upstream) -> Duration {
    let (counted, served) = (proxy.requests().len(), up.served.load(Ordering::Relaxed));
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
    assert!(up.served.load(Ordering::Relaxed) >= served + BURST as u64);
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

    // The cut comes with a burst in flight.
    up.delay_ms.store(400, Ordering::Relaxed);
    let in_flight = {
        let s3 = s3.clone();
        tokio::spawn(async move { burst(&s3, "in-flight").await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    proxy.cut();
    let served_at_cut = up.served.load(Ordering::Relaxed);
    let ok = in_flight.await.unwrap();
    assert!(ok < BURST, "the cut killed none of the burst ({ok} ok)");

    // While cut, every request fails fast and nothing reaches upstream.
    up.delay_ms.store(0, Ordering::Relaxed);
    let counted = proxy.requests().len();
    let t0 = Instant::now();
    assert_eq!(
        burst(&s3, "while-cut").await,
        0,
        "a GET went through the cut"
    );
    assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
    assert_eq!(proxy.requests().len(), counted, "the cut relayed a request");
    // (A response the upstream had already begun writing may complete
    // upstream; none may start.)
    assert!(up.served.load(Ordering::Relaxed) <= served_at_cut + BURST as u64);

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

    // A firewall DROP with a burst in flight: nothing is answered.
    up.delay_ms.store(400, Ordering::Relaxed);
    let in_flight = {
        let s3 = s3.clone();
        tokio::spawn(async move { burst(&s3, "in-flight").await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    proxy.blackhole();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!in_flight.is_finished(), "a black-holed burst returned");
    up.delay_ms.store(0, Ordering::Relaxed);

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
