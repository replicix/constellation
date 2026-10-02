//! Plan 37 K0 Track A: the CSI `NodeStageVolume` shape, against the real
//! daemon.
//!
//! The node plugin half of plan 37 §4/§8: *this* process calls `mount(2)`
//! on a `/dev/fuse` descriptor it opened itself
//! (`constellation_platform::linux::fuse_mount_fd`, the privileged step)
//! and hands that descriptor to an already-running `constellation` daemon
//! over its control socket — `view.mount` with
//! `MountSource::PreopenedFd`, which rides `SCM_RIGHTS` — so the daemon
//! serves a mount it never mounted. Then it reads and writes through the
//! mountpoint and detaches the view again.
//!
//! What `handover_probe` (`crates/frontend-fuse/examples/`) does for the
//! *handover*, this does for the *staging*: the same primitive, but with
//! the product's control protocol, authz and engine in the path instead
//! of a probe socket and a toy `Vfs`.
//!
//! Needs root (for `mount(2)`) and a daemon to talk to, e.g.
//!
//! ```text
//! constellation fs create k0a --s3 file:///work/s3
//! constellation mount / /work/mnt-a --s3 file:///work/s3 \
//!     --state-dir /work/state --foreground &
//! stage_volume_probe --state-dir /work/state --mountpoint /work/mnt-b
//! ```

use anyhow::{bail, Context, Result};
use constellation_control::methods as cm;
use constellation_control::proto::types::{
    MountSource, MountViewOpts, ViewMountParams, ViewUnmountParams,
};
use constellation_control::transport::locate_socket;
use constellation_control::Client;
use std::path::{Path, PathBuf};

fn arg(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let at = args.iter().position(|a| a == name)?;
    args.get(at + 1).cloned()
}

#[tokio::main]
async fn main() -> Result<()> {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        bail!("this probe calls mount(2) itself: run it as root, with /dev/fuse");
    }
    let state_dir = PathBuf::from(arg("--state-dir").context("--state-dir is required")?);
    let mountpoint = PathBuf::from(arg("--mountpoint").context("--mountpoint is required")?);
    let subtree = arg("--subtree").unwrap_or_else(|| "/".into());
    std::fs::create_dir_all(&mountpoint)?;

    let socket = locate_socket(&state_dir)
        .with_context(|| format!("no control socket recorded in {}", state_dir.display()))?;

    // The privileged step, exactly as `NodeStageVolume` will do it.
    let mut opts = constellation_platform::MountOpts::new("constellation".to_string());
    opts.allow_other = true;
    let fd = constellation_platform::linux::fuse_mount_fd(&mountpoint, &opts)
        .with_context(|| format!("mounting FUSE at {}", mountpoint.display()))?;
    println!(
        "mounted {} with mount(2); handing the fd over",
        mountpoint.display()
    );

    let client = Client::connect_unix(&socket).await?;
    let view = client
        .call_with_fd::<cm::ViewMount>(
            ViewMountParams {
                subtree: subtree.clone(),
                source: MountSource::PreopenedFd {
                    mountpoint: Some(mountpoint.clone()),
                    opts: MountViewOpts {
                        allow_other: true,
                        ..Default::default()
                    },
                },
                labels: [("pv".to_string(), "k0a-probe".to_string())].into(),
                qos: Default::default(),
                confine_links: false,
            },
            fd,
        )
        .await
        .context("view.mount with a preopened descriptor")?;
    println!(
        "view {} attached: subtree {}, known as {}",
        view.id, view.subtree, view.mountpoint
    );

    // The daemon serves the mount this process made. Everything below is
    // ordinary file I/O on it.
    let outcome = exercise(&mountpoint);

    // Detaching it again (K0's gap 3, closed by 37-K3a): the daemon knows
    // the view by the mountpoint sent with it, and `view.unmount` ends the
    // session — publishing pending writes, closing the connection — since
    // it did not make the mount and does not unmount it.
    let detached = client
        .call_bounded::<cm::ViewUnmount>(
            ViewUnmountParams {
                mountpoint: PathBuf::from(&view.mountpoint),
            },
            std::time::Duration::from_secs(10),
        )
        .await;
    match &detached {
        Ok(_) => println!("view.unmount: ok"),
        Err(e) => println!("view.unmount: {e}"),
    }
    // The node plugin's own half: it made the mount, so it unmounts it.
    unmount(&mountpoint).context("umount2 of the staged mountpoint")?;
    println!("unmounted {} by path", mountpoint.display());
    outcome?;
    let left = client
        .call_bounded::<cm::ViewList>(Default::default(), std::time::Duration::from_secs(10))
        .await
        .context("view.list")?;
    println!(
        "views left: {:?}",
        left.views.iter().map(|v| &v.mountpoint).collect::<Vec<_>>()
    );
    Ok(())
}

fn unmount(path: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: a valid NUL-terminated path that outlives the call.
    if unsafe { libc::umount2(c.as_ptr(), 0) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

/// Write, read back and list through the handed-over mount.
fn exercise(mountpoint: &Path) -> Result<()> {
    let file = mountpoint.join("k0a-staged.txt");
    let body = b"staged through a preopened /dev/fuse descriptor\n";
    std::fs::write(&file, body).context("writing through the handed-over mount")?;
    let back = std::fs::read(&file).context("reading it back")?;
    if back != body {
        bail!("what came back is not what went in ({} bytes)", back.len());
    }
    let listed = std::fs::read_dir(mountpoint)?.count();
    let md = std::fs::metadata(&file)?;
    println!(
        "wrote and read back {} bytes, {} entries in the root, size {}",
        body.len(),
        listed,
        md.len()
    );
    std::fs::remove_file(&file)?;
    Ok(())
}
