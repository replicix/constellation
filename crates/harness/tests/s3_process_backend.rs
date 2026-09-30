//! The `process` S3 backend (native versitygw + toxiproxy-server) behaves
//! like the docker one (floci + dockerised toxiproxy) as seen through
//! `S3Env`: the bucket exists, the daemons' own S3 client can PUT/GET through
//! the toxiproxy endpoint, a latency toxic is observable, a cut and heal
//! work, the harness's raw (SigV4-signed) requests work against the direct
//! endpoint, and teardown leaves nothing listening. The same checks run
//! against both backends.
//!
//! Needs the two native binaries (`tests/ci/install-native-s3.sh`; found via
//! `CONSTELLATION_VERSITYGW_BIN` / `CONSTELLATION_TOXIPROXY_BIN`, `PATH` or
//! `~/.local/bin`), so it is `#[ignore]`d by default:
//!
//!   cargo test -p constellation-harness --test s3_process_backend -- --ignored

use constellation_harness::s3env::{self, S3Backend, S3Env, BUCKET};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

fn store(endpoint: &str) -> impl ObjectStore {
    let builder = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_access_key_id("test")
        .with_secret_access_key("test");
    // The daemons' client and the retry budget `Client::cmd` gives them.
    constellation_store_s3::configure_s3_client(builder)
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            retry_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .build()
        .unwrap()
}

fn listening(url: &str) -> bool {
    let addr: SocketAddr = url.trim_start_matches("http://").parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
}

#[test]
#[ignore = "needs native versitygw + toxiproxy-server (tests/ci/install-native-s3.sh)"]
fn process_backend_put_get_latency_cut_teardown() {
    exercise(S3Backend::Process);
}

/// The same checks against the default docker backend (floci + dockerised
/// toxiproxy), which is what makes "identical through `S3Env`" a tested
/// claim rather than a hope.
#[test]
#[ignore = "needs docker and the floci/toxiproxy images"]
fn docker_backend_put_get_latency_cut_teardown() {
    exercise(S3Backend::Docker);
}

/// `set_backend` and the `s3auth` helpers act on one process-wide backend,
/// so the two backends' checks (both run by `-- --ignored`, in parallel by
/// default) must not interleave, and neither may run while
/// `missing_binaries_name_the_install_script` has `PATH` pointed away.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn exercise(backend: S3Backend) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    s3env::set_backend(backend);
    let env = S3Env::start().expect("starting the S3 backend");
    assert_eq!(env.backend(), backend);
    let (endpoint, direct) = (env.endpoint.clone(), env.direct_endpoint.clone());
    assert_ne!(endpoint, direct, "clients go through the proxy");
    let proxy = env.s3_proxy().expect("creating the s3 proxy");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let s3 = store(&endpoint);
    let key = Path::from("harness-check/obj");
    let body: Vec<u8> = (0..(1 << 20)).map(|i| (i % 251) as u8).collect();

    // PUT/GET through the toxiproxy endpoint, the bucket having been
    // created by S3Env::start.
    rt.block_on(async {
        s3.put(&key, body.clone().into())
            .await
            .expect("PUT via proxy");
        let got = s3
            .get(&key)
            .await
            .expect("GET via proxy")
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.as_ref(), &body[..]);
    });

    // Latency toxic: `latency(ms)` applies in both directions, so a GET costs
    // at least two of them.
    let t = Instant::now();
    rt.block_on(async { s3.get(&key).await.unwrap().bytes().await.unwrap() });
    let base = t.elapsed();
    proxy.latency(250, 0).expect("adding latency toxic");
    let t = Instant::now();
    rt.block_on(async { s3.get(&key).await.unwrap().bytes().await.unwrap() });
    let slow = t.elapsed();
    assert!(
        slow >= Duration::from_millis(450) && slow > base + Duration::from_millis(300),
        "latency toxic not observed: baseline {base:?}, with toxic {slow:?}"
    );
    proxy.heal().unwrap();
    let t = Instant::now();
    rt.block_on(async { s3.get(&key).await.unwrap().bytes().await.unwrap() });
    assert!(
        t.elapsed() < Duration::from_millis(400),
        "toxic not removed by heal"
    );

    // Cut: requests fail; heal: they work again.
    proxy.cut().unwrap();
    assert!(
        rt.block_on(async { s3.get(&key).await }).is_err(),
        "GET succeeded while cut"
    );
    proxy.heal().unwrap();
    rt.block_on(async {
        s3.get(&key)
            .await
            .expect("GET after heal")
            .bytes()
            .await
            .unwrap()
    });

    // The harness's raw helpers (SigV4-signed for this backend) see the
    // object on the direct endpoint, and can delete it.
    let raw = format!("{direct}/{BUCKET}/harness-check/obj");
    let listing = constellation_harness::s3auth::get(&format!(
        "{direct}/{BUCKET}?list-type=2&prefix=harness-check/"
    ))
    .call()
    .expect("raw signed LIST")
    .into_string()
    .unwrap();
    assert!(
        listing.contains("<Key>harness-check/obj</Key>"),
        "{listing}"
    );
    assert_eq!(
        constellation_harness::s3auth::head(&raw)
            .call()
            .unwrap()
            .status(),
        200
    );
    constellation_harness::s3auth::delete(&raw)
        .call()
        .expect("raw DELETE");
    assert!(constellation_harness::s3auth::head(&raw).call().is_err());
    // ...and versitygw refuses unsigned requests, which is why they are signed.
    if backend == S3Backend::Process {
        assert!(ureq::get(&raw).call().is_err());
    }

    // Teardown: dropping the env stops both servers.
    assert!(listening(&endpoint) && listening(&direct));
    drop(env);
    assert!(
        !listening(&endpoint),
        "toxiproxy still listening after teardown"
    );
    assert!(
        !listening(&direct),
        "versitygw still listening after teardown"
    );
}

#[test]
fn missing_binaries_name_the_install_script() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let saved: Vec<_> = [
        "PATH",
        "HOME",
        "CONSTELLATION_VERSITYGW_BIN",
        "CONSTELLATION_TOXIPROXY_BIN",
    ]
    .iter()
    .map(|k| (*k, std::env::var_os(k)))
    .collect();
    // SAFETY: the other tests of this binary are serialised by `SERIAL`
    // (and spawn no threads that outlive them); env is restored below.
    unsafe {
        std::env::set_var("PATH", "/nonexistent");
        std::env::set_var("HOME", "/nonexistent");
        std::env::remove_var("CONSTELLATION_VERSITYGW_BIN");
        std::env::remove_var("CONSTELLATION_TOXIPROXY_BIN");
    }
    let err = S3Env::start_with(S3Backend::Process)
        .err()
        .map(|e| format!("{e:#}"));
    for (k, v) in saved {
        // SAFETY: as above.
        unsafe {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
    let err = err.expect("start must fail without the binaries");
    assert!(err.contains("tests/ci/install-native-s3.sh"), "{err}");
    assert!(err.contains("CONSTELLATION_VERSITYGW_BIN"), "{err}");
}
