//! Test environment: floci (S3) + toxiproxy on a private docker
//! network. Constellation clients talk to S3 *through* the proxy, so
//! every scenario can inject faults on the S3 path.
//!
//!   client (host) -> 127.0.0.1:<toxi-port> -> toxiproxy -> floci:4566

use crate::docker::{Container, Network};
use crate::reqlog::CountingProxy;
use crate::toxiproxy::{Proxy, Toxiproxy};
use anyhow::{Context, Result};

pub const FLOCI_IMAGE: &str = "floci/floci:1.7.0-compat";
pub const TOXIPROXY_IMAGE: &str = "ghcr.io/shopify/toxiproxy:2.12.0";
pub const BUCKET: &str = "constellation-harness";

/// Prefix for the docker container and network names this environment
/// owns, from `CONSTELLATION_HARNESS_DOCKER_PREFIX` (default
/// `constellation-harness`).
///
/// Startup force-removes leftovers under its own prefix, because a
/// crashed run holds the network open. That makes two harness processes
/// on one host mutually destructive: the second one's cleanup deletes the
/// first one's live S3 out from under it, and the first one then fails
/// with connection-refused somewhere unrelated. Giving each run a
/// distinct prefix is the way to run two at once (e.g. a long matrix in
/// one terminal and a single scenario in another).
fn docker_prefix() -> String {
    std::env::var("CONSTELLATION_HARNESS_DOCKER_PREFIX")
        .unwrap_or_else(|_| "constellation-harness".to_string())
}

pub struct S3Env {
    // Drop order matters: proxy state -> containers -> network.
    pub toxiproxy: Toxiproxy,
    /// Container name of the S3 emulator, as toxiproxy resolves it on the
    /// private network.
    floci_name: String,
    _toxi: Container,
    _floci: Container,
    _net: Network,
    /// S3 endpoint (through the proxy) for constellation clients.
    pub endpoint: String,
    /// S3 endpoint bypassing the proxy (for harness-side checks).
    #[allow(dead_code)]
    pub direct_endpoint: String,
}

impl S3Env {
    pub fn start() -> Result<S3Env> {
        let prefix = docker_prefix();
        let floci_name = format!("{prefix}-floci");
        let toxi_name = format!("{prefix}-toxiproxy");
        // Leftovers from a crashed run would hold the network open.
        let _ = crate::docker::docker(&["rm", "-f", &floci_name, &toxi_name]);
        let net = Network::create(&prefix)?;
        let floci = Container::run(
            &floci_name,
            FLOCI_IMAGE,
            &[
                "--network",
                &net.name,
                "-p",
                "127.0.0.1::4566",
                "-e",
                "FLOCI_STORAGE_MODE=memory",
            ],
        )?;
        let toxi = Container::run(
            &toxi_name,
            TOXIPROXY_IMAGE,
            &[
                "--network",
                &net.name,
                "-p",
                "127.0.0.1::8474",
                "-p",
                "127.0.0.1::4567",
            ],
        )?;
        let toxi_api = toxi.host_port(8474)?;
        let s3_via_proxy = toxi.host_port(4567)?;
        let s3_direct = floci.host_port(4566)?;

        let toxiproxy = Toxiproxy::new(toxi_api);
        toxiproxy.wait_ready(30)?;

        let direct_endpoint = format!("http://127.0.0.1:{s3_direct}");
        wait_s3_ready(&direct_endpoint, 60)?;
        create_bucket(&direct_endpoint, BUCKET)?;

        Ok(S3Env {
            toxiproxy,
            floci_name,
            _toxi: toxi,
            _floci: floci,
            _net: net,
            endpoint: format!("http://127.0.0.1:{s3_via_proxy}"),
            direct_endpoint,
        })
    }

    /// The S3 proxy all constellation clients go through.
    pub fn s3_proxy(&self) -> Result<Proxy<'_>> {
        self.toxiproxy
            .create_proxy("s3", "0.0.0.0:4567", &format!("{}:4566", self.floci_name))
    }

    /// A request-counting relay chained *in front of* toxiproxy, for
    /// scenarios that assert on S3 request classes (plan 26). Clients
    /// given `counter.endpoint()` still go through every toxic:
    ///
    ///   client -> counter (host) -> toxiproxy -> floci
    pub fn counting_proxy(&self) -> Result<CountingProxy> {
        let upstream = self
            .endpoint
            .strip_prefix("http://")
            .context("S3 endpoint is not http://")?;
        CountingProxy::start(upstream)
    }
}

fn wait_s3_ready(endpoint: &str, timeout_s: u64) -> Result<()> {
    for _ in 0..timeout_s * 10 {
        if let Ok(resp) = ureq::get(&format!("{endpoint}/_floci/health")).call() {
            if resp.status() == 200 {
                return Ok(());
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    anyhow::bail!("floci not ready after {timeout_s}s")
}

/// Path-style anonymous CreateBucket (floci does not enforce SigV4 by
/// default).
fn create_bucket(endpoint: &str, bucket: &str) -> Result<()> {
    match ureq::put(&format!("{endpoint}/{bucket}")).call() {
        Ok(_) => Ok(()),
        // 409 = already exists (rerun); anything else is fatal.
        Err(ureq::Error::Status(409, _)) => Ok(()),
        Err(e) => Err(e).context("creating harness bucket"),
    }
}
