//! External suite runners (fio, stress-ng) driven by the harness so the
//! same compliance/performance tools that run in the clean containerized
//! lane can also run under injected faults.

use anyhow::{bail, Result};
use std::path::Path;
use std::process::Command;

pub fn have(bin: &str) -> bool {
    Command::new("which")
        .arg(bin)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn run(mut cmd: Command, what: &str) -> Result<()> {
    let out = cmd.output()?;
    if !out.status.success() {
        bail!(
            "{what} failed ({}):\n{}\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// fio random write with crc32c end-to-end verification: any corruption
/// or lost write under the injected fault surfaces as a verify error.
pub fn fio_verify(mnt: &Path, size: &str, jobs: u32) -> Result<()> {
    let mut cmd = Command::new("fio");
    cmd.arg("--name=harness-verify")
        .arg(format!("--directory={}", mnt.display()))
        .arg(format!("--size={size}"))
        .arg(format!("--numjobs={jobs}"))
        .args([
            "--rw=randwrite",
            "--bsrange=4k-256k",
            "--ioengine=psync",
            "--fallocate=none",
            "--verify=crc32c",
            "--do_verify=1",
            "--end_fsync=1",
            "--group_reporting",
            "--output-format=terse",
        ]);
    run(cmd, "fio verify")
}

/// stress-ng filesystem stressors (metadata churn) on the mount.
pub fn stress_ng(mnt: &Path, stressors: &[&str], timeout_s: u32) -> Result<()> {
    for s in stressors {
        let mut cmd = Command::new("stress-ng");
        cmd.arg("--temp-path")
            .arg(mnt)
            .arg(format!("--{s}"))
            .arg("2")
            .arg("--timeout")
            .arg(format!("{timeout_s}s"))
            .arg("--metrics-brief");
        run(cmd, &format!("stress-ng {s}"))?;
    }
    Ok(())
}
