//! Toxiproxy HTTP API client: create proxies and inject network faults
//! (latency, bandwidth limits, timeouts, connection cuts) between the
//! constellation client and S3.
//!
//! Every change is also recorded in a process-wide timeline
//! ([`timeline`], [`outage_at`]): the load campaign's triage keeps it with
//! a failed run, and its errno and hang checks excuse what an injected S3
//! outage allows (`ENOLCK` for a new lock, an `fsync` waiting it out).
//! Toxics named `load-*` belong to the load profile ([`crate::load`]): a
//! scenario's `heal` leaves them in place.

use anyhow::{Context, Result};
use serde_json::json;

pub struct Toxiproxy {
    api: String,
}

/// One change to a proxy: when, which proxy, what.
#[derive(Debug, Clone)]
pub struct Event {
    pub at: std::time::Instant,
    pub unix_ms: u64,
    pub proxy: String,
    pub what: String,
}

static TIMELINE: std::sync::Mutex<Vec<Event>> = std::sync::Mutex::new(Vec::new());

fn record(proxy: &str, what: String) {
    let unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    TIMELINE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(Event {
            at: std::time::Instant::now(),
            unix_ms,
            proxy: proxy.to_string(),
            what,
        });
}

/// Every proxy change since `since` (all of them: `None`).
pub fn timeline(since: Option<std::time::Instant>) -> Vec<Event> {
    TIMELINE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|e| since.is_none_or(|s| e.at >= s))
        .cloned()
        .collect()
}

/// Whether some proxy was cut or black-holed at any point of `[from, to]`
/// (an outage that began before `from` and was still on counts).
pub fn outage_at(from: std::time::Instant, to: std::time::Instant) -> bool {
    let t = TIMELINE.lock().unwrap_or_else(|e| e.into_inner());
    // (proxy, "cut" or a timeout toxic's name) -> down.
    let mut down: std::collections::HashMap<(String, String), bool> =
        std::collections::HashMap::new();
    for e in t.iter() {
        if e.at > to {
            break;
        }
        let mut words = e.what.split_whitespace();
        let (key, is_down) = match (words.next(), words.next(), words.next()) {
            (Some("cut"), _, _) => ("cut".to_string(), true),
            (Some("heal"), _, _) => ("cut".to_string(), false),
            (Some("toxic"), Some(name), Some("timeout")) => (name.to_string(), true),
            (Some("remove"), Some(name), _) => (name.to_string(), false),
            _ => continue,
        };
        if is_down && e.at >= from {
            return true;
        }
        down.insert((e.proxy.clone(), key), is_down);
    }
    down.values().any(|d| *d)
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

    /// A client of the API at `api` (`http://host:port`, [`Self::api`]).
    pub fn at(api: &str) -> Self {
        Self {
            api: api.to_string(),
        }
    }

    /// The API's URL.
    pub fn api(&self) -> &str {
        &self.api
    }

    /// One readiness probe of the API.
    pub fn is_ready(&self) -> bool {
        ureq::get(&format!("{}/version", self.api)).call().is_ok()
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
        record(name, format!("create {listen} -> {upstream}"));
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
        record(&self.name, if enabled { "heal" } else { "cut" }.to_string());
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
        record(&self.name, format!("toxic {name} {kind} {attrs}"));
        Ok(())
    }

    /// A load-profile toxic (`load-<kind>-down`, and `-up` when `both`):
    /// kept across [`Self::heal`] and [`Self::remove_all_toxics`].
    pub fn load_toxic(&self, kind: &str, attrs: serde_json::Value, both: bool) -> Result<()> {
        let streams: &[&str] = if both {
            &["downstream", "upstream"]
        } else {
            &["downstream"]
        };
        for stream in streams {
            let name = format!(
                "{}{kind}-{}",
                crate::load::TOXIC_PREFIX,
                &stream[..stream.len() - "stream".len()]
            );
            ureq::post(&format!("{}/proxies/{}/toxics", self.tp.api, self.name))
                .send_json(json!({
                    "name": name,
                    "type": kind,
                    "stream": stream,
                    "toxicity": 1.0,
                    "attributes": attrs,
                }))
                .with_context(|| format!("adding load toxic {kind}"))?;
            record(&self.name, format!("toxic {name} {kind} {attrs}"));
        }
        Ok(())
    }

    /// Remove what [`Self::load_toxic`] added.
    pub fn remove_load_toxic(&self, kind: &str, both: bool) -> Result<()> {
        let streams: &[&str] = if both { &["down", "up"] } else { &["down"] };
        for s in streams {
            let name = format!("{}{kind}-{s}", crate::load::TOXIC_PREFIX);
            let _ = ureq::delete(&format!(
                "{}/proxies/{}/toxics/{}",
                self.tp.api, self.name, name
            ))
            .call();
            record(&self.name, format!("remove {name}"));
        }
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
        record(
            &self.name,
            format!("toxic lat-up latency {ms}+-{jitter_ms}"),
        );
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
                    if name.starts_with(crate::load::TOXIC_PREFIX) {
                        continue;
                    }
                    record(&self.name, format!("remove {name}"));
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
