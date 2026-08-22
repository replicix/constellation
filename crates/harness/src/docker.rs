//! Minimal docker CLI wrapper: containers, networks, logs. Uses the
//! `docker` binary so behavior matches what a developer can replay by
//! hand; no daemon-API client dependency.

use anyhow::{bail, Context, Result};
use std::process::Command;

pub fn docker(args: &[&str]) -> Result<String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .context("running docker (is it installed?)")?;
    if !out.status.success() {
        bail!(
            "docker {:?} failed: {}",
            args.first().unwrap_or(&""),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// A container removed on drop (harness crash leaves nothing behind).
pub struct Container {
    #[allow(dead_code)]
    pub id: String,
    pub name: String,
}

impl Container {
    /// `docker run -d` with standard harness labels; returns once created.
    pub fn run(name: &str, image: &str, extra: &[&str]) -> Result<Self> {
        // Remove leftovers from a previous crashed run.
        let _ = docker(&["rm", "-f", name]);
        let mut args = vec![
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "--label",
            "constellation-harness=1",
        ];
        args.extend_from_slice(extra);
        args.push(image);
        let id = docker(&args)?;
        Ok(Self {
            id,
            name: name.to_string(),
        })
    }

    /// Host port mapped for a container port (`-p 127.0.0.1::<port>`).
    pub fn host_port(&self, container_port: u16) -> Result<u16> {
        let out = docker(&["port", &self.name, &container_port.to_string()])?;
        // e.g. "127.0.0.1:32768"
        let port = out
            .lines()
            .next()
            .and_then(|l| l.rsplit(':').next())
            .and_then(|p| p.parse().ok())
            .with_context(|| format!("parsing docker port output {out:?}"))?;
        Ok(port)
    }

    /// Wait until the container reports healthy (needs a HEALTHCHECK).
    #[allow(dead_code)]
    pub fn wait_healthy(&self, timeout_s: u64) -> Result<()> {
        for _ in 0..timeout_s * 10 {
            let st = docker(&["inspect", "-f", "{{.State.Health.Status}}", &self.name])
                .unwrap_or_default();
            match st.as_str() {
                "healthy" => return Ok(()),
                "unhealthy" => bail!("container {} became unhealthy", self.name),
                _ => std::thread::sleep(std::time::Duration::from_millis(100)),
            }
        }
        bail!("container {} not healthy after {timeout_s}s", self.name)
    }

    #[allow(dead_code)]
    pub fn logs(&self) -> String {
        Command::new("docker")
            .args(["logs", "--tail", "100", &self.name])
            .output()
            .map(|o| {
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&o.stdout),
                    String::from_utf8_lossy(&o.stderr)
                )
            })
            .unwrap_or_default()
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

/// A user-defined bridge network removed on drop.
pub struct Network {
    pub name: String,
}

impl Network {
    pub fn create(name: &str) -> Result<Self> {
        let _ = docker(&["network", "rm", name]);
        if docker(&["network", "create", name]).is_err() {
            // Still exists (e.g. attached containers from a crashed
            // run survived): reuse it if it is actually there.
            docker(&["network", "inspect", name])
                .map_err(|e| anyhow::anyhow!("network {name} unusable: {e}"))?;
        }
        Ok(Self {
            name: name.to_string(),
        })
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.name]);
    }
}
