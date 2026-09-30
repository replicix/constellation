//! Plan 31 §6.12: subtree confinement through a real kernel mount. One
//! daemon serves the whole filesystem, a volume view
//! (`/volumes/pv-1 --confine-links`) and a maintenance view (`/
//! --confine-links`), the volumes marked as link domains; the checks are
//! the in-process ones of `constellation-engine`'s `view::confine_tests`,
//! made through the kernel's own path resolution.

use super::{one_client, setup, ts};
use crate::client::is_mountpoint;
use anyhow::{bail, ensure, Context, Result};
use constellation_types::Code;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const DOMAIN_XATTR: &str = "trusted.constellation.link_domain";

fn cstr(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).unwrap()
}

#[cfg(target_os = "linux")]
fn set_xattr(path: &Path, name: &str, value: &[u8]) -> Result<()> {
    let name = CString::new(name).unwrap();
    // SAFETY: valid NUL-terminated strings and a live buffer.
    let rc = unsafe {
        libc::setxattr(
            cstr(path).as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    };
    if rc != 0 {
        bail!(
            "setxattr {name:?} on {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn get_xattr(path: &Path, name: &str) -> Result<Vec<u8>> {
    let name = CString::new(name).unwrap();
    let mut buf = vec![0u8; 256];
    // SAFETY: as above; the kernel writes at most `buf.len()` bytes.
    let n = unsafe {
        libc::getxattr(
            cstr(path).as_ptr(),
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            buf.len(),
        )
    };
    if n < 0 {
        bail!(
            "getxattr {name:?} on {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    buf.truncate(n as usize);
    Ok(buf)
}

// The Linux-only calls (xattr arities, `AT_EMPTY_PATH`): the scenario
// runs on Linux; other hosts only type-check it (`make check-cross`).
#[cfg(not(target_os = "linux"))]
fn set_xattr(_: &Path, _: &str, _: &[u8]) -> Result<()> {
    bail!("Linux only")
}

#[cfg(not(target_os = "linux"))]
fn get_xattr(_: &Path, _: &str) -> Result<Vec<u8>> {
    bail!("Linux only")
}

/// `linkat(fd, "", AT_FDCWD, to, AT_EMPTY_PATH)`: link the open file
/// itself, whatever its names now are. Returns the result and `errno`.
#[cfg(target_os = "linux")]
fn link_fd(file: &std::fs::File, to: &Path) -> (i32, std::io::Error) {
    use std::os::fd::AsRawFd;
    let target = cstr(to);
    let empty = CString::new("").unwrap();
    // SAFETY: a live descriptor and NUL-terminated paths.
    let rc = unsafe {
        libc::linkat(
            file.as_raw_fd(),
            empty.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::AT_EMPTY_PATH,
        )
    };
    (rc, std::io::Error::last_os_error())
}

#[cfg(not(target_os = "linux"))]
fn link_fd(_: &std::fs::File, _: &Path) -> (i32, std::io::Error) {
    (-1, std::io::Error::other("Linux only"))
}

/// `link(from, to)` must fail with EXDEV.
fn expect_exdev(from: &Path, to: &Path) -> Result<()> {
    match std::fs::hard_link(from, to) {
        Ok(()) => bail!(
            "link {} -> {} succeeded; expected EXDEV",
            from.display(),
            to.display()
        ),
        Err(e) if Code::from_io_error(&e) == Code::CrossDevice => Ok(()),
        Err(e) => bail!(
            "link {} -> {}: {e}; expected EXDEV",
            from.display(),
            to.display()
        ),
    }
}

fn names(dir: &Path) -> Result<Vec<String>> {
    let mut out: Vec<String> = std::fs::read_dir(dir)
        .with_context(|| format!("listing {}", dir.display()))?
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<_>>()?;
    out.sort();
    Ok(out)
}

pub(super) fn subtree_confinement(_seed: u64) -> Result<()> {
    let (env, root) = setup("confinement")?;
    let _proxy = env.s3_proxy()?;
    let mut c = one_client(&env, root.path(), &format!("confine-{}", ts()))?;
    let whole = c.mnt.clone();

    // The pool: two volumes marked as link domains, and a loose file.
    let pv1 = whole.join("volumes/pv-1");
    let pv2 = whole.join("volumes/pv-2");
    std::fs::create_dir_all(pv1.join("sub"))?;
    std::fs::create_dir_all(&pv2)?;
    std::fs::write(pv1.join("data.txt"), b"pv-1 data")?;
    std::fs::write(pv2.join("secret.txt"), b"pv-2 secret")?;
    std::fs::write(whole.join("top.txt"), b"top")?;
    set_xattr(&pv1, DOMAIN_XATTR, b"1")?;
    set_xattr(&pv2, DOMAIN_XATTR, b"1")?;
    c.snapshot_create("/volumes/pv-1@own")?;
    c.snapshot_create("/volumes/pv-2@sibling")?;
    c.snapshot_create("/@whole")?;

    // Two more views of the same daemon, attached from fresh CLI calls.
    let vol = root.path().join("vol");
    let maint = root.path().join("maint");
    let mut attached: Vec<PathBuf> = Vec::new();
    let result = (|| -> Result<()> {
        for (inner, mnt) in [("/volumes/pv-1", &vol), ("/", &maint)] {
            std::fs::create_dir_all(mnt)?;
            attach(&c, &env.endpoint, inner, mnt)?;
            attached.push(mnt.clone());
        }
        let status = c.control_status()?;
        ensure!(
            status["mounts"].as_array().map(|m| m.len()) == Some(3),
            "one daemon should serve three views: {status}"
        );
        checks(&whole, &vol, &maint)
    })();
    for mnt in attached.iter().rev() {
        let _ = Command::new("fusermount3").arg("-u").arg(mnt).status();
        let deadline = Instant::now() + Duration::from_secs(30);
        while is_mountpoint(mnt) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    let unmounted = c.unmount();
    result?;
    unmounted
}

/// `constellation mount INNER MNT --confine-links` against `c`'s state
/// dir: attaches to `c`'s daemon (it holds `daemon.lock`).
fn attach(c: &crate::client::Client, endpoint: &str, inner: &str, mnt: &Path) -> Result<()> {
    let bin = std::env::var_os("CONSTELLATION_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("target"))
                .join("release/constellation")
        });
    let out = Command::new(bin)
        .args([
            "mount",
            inner,
            mnt.to_str().unwrap(),
            "--s3",
            &c.backend,
            "--state-dir",
            c.state_dir().to_str().unwrap(),
            "--confine-links",
        ])
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .env("AWS_ENDPOINT", endpoint)
        .env("AWS_ALLOW_HTTP", "true")
        .output()?;
    ensure!(
        out.status.success(),
        "attaching {inner} at {}: {}",
        mnt.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !is_mountpoint(mnt) {
        ensure!(
            Instant::now() < deadline,
            "{} did not come up",
            mnt.display()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn checks(whole: &Path, vol: &Path, maint: &Path) -> Result<()> {
    // `..` at the volume view's root: the kernel leaves the mount for the
    // host directory holding the mountpoint; nothing of the filesystem
    // above the volume is ever listed.
    let parent = vol.parent().unwrap();
    let up = std::fs::metadata(vol.join(".."))?;
    let host = std::fs::metadata(parent)?;
    ensure!(
        (up.dev(), up.ino()) == (host.dev(), host.ino()),
        "`..` of the volume mount is not the host's parent directory"
    );
    let up_names = names(&vol.join(".."))?;
    for leaked in ["pv-2", "top.txt", "volumes"] {
        ensure!(
            !up_names.iter().any(|n| n == leaked),
            "`ls ..` at the volume root shows {leaked}: {up_names:?}"
        );
    }
    ensure!(
        names(vol)? == ["data.txt", "sub"],
        "volume view: {:?}",
        names(vol)?
    );
    let sub_up = std::fs::metadata(vol.join("sub/.."))?;
    let vol_root = std::fs::metadata(vol)?;
    ensure!(
        sub_up.ino() == vol_root.ino(),
        "sub/.. is the volume's root"
    );
    ensure!(std::fs::read(vol.join("data.txt"))? == b"pv-1 data");

    // `.constellation`: the volume's own history, and the root snapshot
    // mirrored at the volume's path — never the sibling's.
    let snaps = vol.join(".constellation/snapshot");
    ensure!(
        names(&snaps)? == ["own", "whole"],
        "snapshots under the volume view: {:?}",
        names(&snaps)?
    );
    ensure!(
        std::fs::metadata(snaps.join("sibling")).is_err(),
        "the sibling volume's snapshot is reachable"
    );
    for snap in ["own", "whole"] {
        ensure!(
            names(&snaps.join(snap))? == ["data.txt", "sub"],
            "{snap}: {:?}",
            names(&snaps.join(snap))?
        );
        ensure!(std::fs::read(snaps.join(snap).join("data.txt"))? == b"pv-1 data");
        ensure!(std::fs::metadata(snaps.join(snap).join("top.txt")).is_err());
    }

    // Hard links, volume view (`--confine-links`): within the volume,
    // POSIX; into another mount, the kernel's own EXDEV.
    std::fs::hard_link(vol.join("data.txt"), vol.join("sub/data-link"))?;
    ensure!(std::fs::metadata(vol.join("data.txt"))?.nlink() == 2);
    expect_exdev(&whole.join("volumes/pv-2/secret.txt"), &vol.join("stolen"))?;
    // A file the volume view has open is moved out of the volume by the
    // root view; linking it back through the open descriptor reaches the
    // view with a handle that has no name inside: EXDEV.
    std::fs::write(vol.join("moving.txt"), b"moving")?;
    let open = std::fs::File::open(vol.join("moving.txt"))?;
    std::fs::rename(
        whole.join("volumes/pv-1/moving.txt"),
        whole.join("volumes/pv-2/moving.txt"),
    )?;
    let (rc, err) = link_fd(&open, &vol.join("moved-back"));
    ensure!(
        rc != 0 && Code::from_io_error(&err) == Code::CrossDevice,
        "linkat of a handle moved out of the volume: rc {rc}, {err}; expected EXDEV"
    );
    drop(open);

    // Maintenance view (`/ --confine-links`): the marked volumes are
    // link-disjoint; within one, and in the unmarked rest, POSIX.
    let m1 = maint.join("volumes/pv-1");
    let m2 = maint.join("volumes/pv-2");
    expect_exdev(&m1.join("data.txt"), &m2.join("x"))?;
    expect_exdev(&m2.join("secret.txt"), &m1.join("x"))?;
    expect_exdev(&m1.join("data.txt"), &maint.join("x"))?;
    std::fs::hard_link(maint.join("top.txt"), maint.join("top-link"))?;
    std::fs::hard_link(m1.join("data.txt"), m1.join("data-2"))?;
    // One of several names of a file cannot move to another volume.
    match std::fs::rename(m1.join("data-2"), m2.join("data-2")) {
        Err(e) if Code::from_io_error(&e) == Code::CrossDevice => {}
        other => bail!("rename of a multiply-linked file across volumes: {other:?}"),
    }
    // The whole-filesystem view without `--confine-links` is POSIX.
    std::fs::hard_link(
        whole.join("volumes/pv-2/secret.txt"),
        whole.join("volumes/pv-2/secret-2"),
    )?;

    // The ordinary file operations through the confined volume view.
    let f = vol.join("ops.txt");
    std::fs::write(&f, b"hello")?;
    ensure!(std::fs::read(&f)? == b"hello");
    std::fs::rename(&f, vol.join("sub/ops2.txt"))?;
    let f = vol.join("sub/ops2.txt");
    set_xattr(&f, "user.color", b"blue")?;
    ensure!(get_xattr(&f, "user.color")? == b"blue");
    let file = std::fs::OpenOptions::new().read(true).open(&f)?;
    {
        use std::os::fd::AsRawFd;
        // SAFETY: a live descriptor.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        ensure!(rc == 0, "flock: {}", std::io::Error::last_os_error());
        // SAFETY: as above.
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    }
    drop(file);
    std::fs::remove_file(&f)?;
    ensure!(std::fs::metadata(&f).is_err());
    // …and they are the root view's too (one replica).
    ensure!(std::fs::read(whole.join("volumes/pv-1/sub/data-link"))? == b"pv-1 data");
    Ok(())
}
