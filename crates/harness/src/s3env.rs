//! Test environment: floci (S3) + toxiproxy on a private docker
//! network. Constellation clients talk to S3 *through* the proxy, so
//! every scenario can inject faults on the S3 path.
//!
//!   client (host) -> 127.0.0.1:<toxi-port> -> toxiproxy -> floci:4566

use crate::docker::{Container, Network};
use crate::toxiproxy::{Proxy, Toxiproxy};
use anyhow::{Context, Result};

pub const FLOCI_IMAGE: &str = "floci/floci:1.7.0-compat";
pub const TOXIPROXY_IMAGE: &str = "ghcr.io/shopify/toxiproxy:2.12.0";
pub const BUCKET: &str = "constellation-harness";

pub struct S3Env {
    // Drop order matters: proxy state -> containers -> network.
    pub toxiproxy: Toxiproxy,
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
        // Leftovers from a crashed run would hold the network open.
        let _ = crate::docker::docker(&["rm", "-f", "harness-floci", "harness-toxiproxy"]);
        let net = Network::create("constellation-harness")?;
        let floci = Container::run(
            "harness-floci",
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
            "harness-toxiproxy",
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
            .create_proxy("s3", "0.0.0.0:4567", "harness-floci:4566")
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
