//! Toxiproxy HTTP API client: create proxies and inject network faults
//! (latency, bandwidth limits, timeouts, connection cuts) between the
//! constellation client and S3.

use anyhow::{Context, Result};
use serde_json::json;

pub struct Toxiproxy {
    api: String,
}

/// One TCP proxy (client -> toxiproxy -> upstream) with togglable faults.
pub struct Proxy<'a> {
    tp: &'a Toxiproxy,
    pub name: String,
}

impl Toxiproxy {
    pub fn new(host_port: u16) -> Self {
        Self {
            api: format!("http://127.0.0.1:{host_port}"),
        }
    }

    pub fn wait_ready(&self, timeout_s: u64) -> Result<()> {
        for _ in 0..timeout_s * 10 {
            if ureq::get(&format!("{}/version", self.api)).call().is_ok() {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        anyhow::bail!("toxiproxy API not ready after {timeout_s}s")
    }

    /// A handle on the proxy `name`, created earlier.
    pub fn proxy(&self, name: &str) -> Proxy<'_> {
        Proxy {
            tp: self,
            name: name.to_string(),
        }
    }

    /// Create a proxy listening on `listen` (inside the toxiproxy
    /// container) forwarding to `upstream`.
    pub fn create_proxy(&self, name: &str, listen: &str, upstream: &str) -> Result<Proxy<'_>> {
        ureq::post(&format!("{}/proxies", self.api))
            .send_json(json!({
                "name": name,
                "listen": listen,
                "upstream": upstream,
                "enabled": true,
            }))
            .context("creating toxiproxy proxy")?;
        Ok(Proxy {
            tp: self,
            name: name.to_string(),
        })
    }
}

impl Proxy<'_> {
    fn set_enabled(&self, enabled: bool) -> Result<()> {
        ureq::patch(&format!("{}/proxies/{}", self.tp.api, self.name))
            .send_json(json!({ "enabled": enabled }))
            .context("toggling proxy")?;
        Ok(())
    }

    /// Hard network cut: connections refused, established ones dropped.
    pub fn cut(&self) -> Result<()> {
        self.set_enabled(false)
    }

    pub fn heal(&self) -> Result<()> {
        self.set_enabled(true)?;
        self.remove_all_toxics()
    }

    fn add_toxic(&self, name: &str, kind: &str, attrs: serde_json::Value) -> Result<()> {
        ureq::post(&format!("{}/proxies/{}/toxics", self.tp.api, self.name))
            .send_json(json!({
                "name": name,
                "type": kind,
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": attrs,
            }))
            .with_context(|| format!("adding toxic {kind}"))?;
        Ok(())
    }

    /// Added latency (both directions get it via separate toxics).
    pub fn latency(&self, ms: u64, jitter_ms: u64) -> Result<()> {
        self.add_toxic(
            "lat-down",
            "latency",
            json!({"latency": ms, "jitter": jitter_ms}),
        )?;
        ureq::post(&format!("{}/proxies/{}/toxics", self.tp.api, self.name))
            .send_json(json!({
                "name": "lat-up",
                "type": "latency",
                "stream": "upstream",
                "toxicity": 1.0,
                "attributes": {"latency": ms, "jitter": jitter_ms},
            }))
            .context("adding upstream latency")?;
        Ok(())
    }

    /// Throttle throughput to `kbps` KB/s.
    pub fn bandwidth(&self, kbps: u64) -> Result<()> {
        self.add_toxic("bw", "bandwidth", json!({"rate": kbps}))
    }

    /// Kill connections after `ms` of data flow (mid-transfer resets).
    #[allow(dead_code)]
    pub fn timeout(&self, ms: u64) -> Result<()> {
        self.add_toxic("timeout", "timeout", json!({"timeout": ms}))
    }

    /// Chop data into tiny delayed slices (stresses partial reads).
    pub fn slicer(&self, avg_bytes: u64, delay_us: u64) -> Result<()> {
        self.add_toxic(
            "slice",
            "slicer",
            json!({"average_size": avg_bytes, "size_variation": avg_bytes / 2, "delay": delay_us}),
        )
    }

    pub fn remove_all_toxics(&self) -> Result<()> {
        let resp: serde_json::Value =
            ureq::get(&format!("{}/proxies/{}/toxics", self.tp.api, self.name))
                .call()
                .context("listing toxics")?
                .into_json()?;
        if let Some(arr) = resp.as_array() {
            for t in arr {
                if let Some(name) = t["name"].as_str() {
                    let _ = ureq::delete(&format!(
                        "{}/proxies/{}/toxics/{}",
                        self.tp.api, self.name, name
                    ))
                    .call();
                }
            }
        }
        Ok(())
    }
}
