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

/// Whether `requirement` is checked by [`unavailable`] rather than
/// [`have`].
pub fn is_platform_requirement(requirement: &str) -> bool {
    requirement == FUSE_URING
}

/// Why a platform requirement is not met here, or `None` when it is (or
/// when `requirement` is a host binary, which [`have`] checks).
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
