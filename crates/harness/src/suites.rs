//! External suite runners (fio, stress-ng) driven by the harness so the
//! same compliance/performance tools that run in the clean containerized
//! lane can also run under injected faults.

use anyhow::{bail, Result};
use std::path::Path;
use std::process::Command;

/// A scenario `requires` entry that is not a host binary: FUSE-over-io_uring
/// granted to an unprivileged mount here — the running kernel offers it
/// (`fuse.enable_uring=Y`, 6.14+), io_uring is not disabled for this user
/// (`kernel.io_uring_disabled=0`), this process may call
/// `io_uring_setup(2)`, and the `constellation` binary under test was built
/// with the `io-uring` feature (`daemon --fuse-transports`). Plan 38 Z2a's
/// ring fault scenarios need all four; elsewhere they skip, saying which
/// is missing.
pub const FUSE_URING: &str = "fuse-uring";

/// Why [`FUSE_URING`] is not met here, or `None` when it is (or when
/// `requirement` is anything else, which [`missing`] checks).
pub fn unavailable(requirement: &str) -> Option<String> {
    if requirement != FUSE_URING {
        return None;
    }
    fuse_uring_unavailable()
}

fn fuse_uring_unavailable() -> Option<String> {
    let read = |p: &str| std::fs::read_to_string(p).map(|v| v.trim().to_string());
    match read("/sys/module/fuse/parameters/enable_uring") {
        Ok(v) if v == "Y" => {}
        Ok(v) => {
            return Some(format!(
                "FUSE-over-io_uring unavailable: fuse.enable_uring={v}"
            ))
        }
        Err(e) => {
            return Some(format!(
                "FUSE-over-io_uring unavailable: no fuse.enable_uring ({e})"
            ))
        }
    }
    if let Ok(v) = read("/proc/sys/kernel/io_uring_disabled") {
        if v != "0" {
            return Some(format!(
                "FUSE-over-io_uring unavailable: kernel.io_uring_disabled={v}"
            ));
        }
    }
    // SAFETY: an io_uring_setup(2) probe with a valid zeroed params struct;
    // the returned descriptor is closed at once.
    let fd = unsafe {
        let mut params = [0u8; 120];
        libc::syscall(libc::SYS_io_uring_setup, 2u32, params.as_mut_ptr())
    };
    if fd < 0 {
        return Some(format!(
            "FUSE-over-io_uring unavailable: io_uring_setup failed ({})",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the descriptor the probe just created.
    unsafe { libc::close(fd as i32) };
    let bin = crate::client::constellation_bin();
    match Command::new(&bin)
        .args(["daemon", "--fuse-transports"])
        .output()
    {
        Ok(out)
            if String::from_utf8_lossy(&out.stdout)
                .lines()
                .any(|l| l == "uring") =>
        {
            None
        }
        Ok(_) => Some(format!(
            "FUSE-over-io_uring unavailable: {} was built without the io-uring feature \
             (--features constellation-frontend-fuse/io-uring)",
            bin.display()
        )),
        Err(e) => Some(format!("{}: {e}", bin.display())),
    }
}

/// A `requires` entry naming a host capability rather than a binary:
/// the process must hold `CAP_SYS_ADMIN` (FUSE passthrough registers
/// backing files with it, plan 38 Z3b).
pub const CAP_SYS_ADMIN: &str = "CAP_SYS_ADMIN";
/// A `requires` entry naming the kernel FUSE passthrough first shipped in.
pub const LINUX_6_9: &str = "linux>=6.9";

/// Why a scenario's `requires` entry is not met on this host, or `None`.
/// A binary is looked up on `PATH`; [`FUSE_URING`], [`CAP_SYS_ADMIN`]
/// and [`LINUX_6_9`] are checked as what they name. Either way the scenario SKIPs loudly,
/// naming the reason.
pub fn missing(req: &str) -> Option<String> {
    match req {
        FUSE_URING => fuse_uring_unavailable(),
        CAP_SYS_ADMIN => (!has_cap_sys_admin())
            .then(|| "requires CAP_SYS_ADMIN (run the harness as root)".to_string()),
        LINUX_6_9 => {
            let release = kernel_release();
            (!kernel_at_least(&release, 6, 9))
                .then(|| format!("requires Linux >= 6.9 (this is {release})"))
        }
        bin => (!have(bin)).then(|| format!("{bin} not installed")),
    }
}

/// `CapEff` of this process holds `CAP_SYS_ADMIN` (bit 21).
pub fn has_cap_sys_admin() -> bool {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("CapEff:"))
                .and_then(|h| u64::from_str_radix(h.trim(), 16).ok())
        })
        .is_some_and(|caps| caps & (1 << 21) != 0)
}

fn kernel_release() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "an unknown kernel".to_string())
}

/// `release` (`uname -r`) is at least `major.minor`.
fn kernel_at_least(release: &str, major: u32, minor: u32) -> bool {
    let mut parts = release
        .split(|c: char| !c.is_ascii_digit())
        .filter(|p| !p.is_empty())
        .map(|p| p.parse::<u32>().unwrap_or(0));
    let (Some(a), Some(b)) = (parts.next(), parts.next()) else {
        return false;
    };
    (a, b) >= (major, minor)
}

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
            // Otherwise fio leaves `local-harness-verify-*-verify.state`
            // in the harness's working directory (the checkout).
            "--verify_state_save=0",
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

#[cfg(test)]
mod requirement_tests {
    use super::*;

    #[test]
    fn kernel_releases_compare_by_major_and_minor() {
        assert!(kernel_at_least("7.3.0-0.rc4.fc46.x86_64", 6, 9));
        assert!(kernel_at_least("6.9.0", 6, 9));
        assert!(!kernel_at_least("6.8.12-300.fc40", 6, 9));
        assert!(kernel_at_least("6.10.1", 6, 9));
        assert!(!kernel_at_least("garbage", 6, 9));
    }
}
