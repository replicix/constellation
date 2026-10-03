//! The K5 kind lane's busy-writer loss, without Kubernetes.
//!
//! A long-lived `O_APPEND` descriptor appends 64 KiB blocks while a second
//! descriptor `fsync`s the file (`sync -d`) and a third party changes its
//! attributes. On kind that third party is the kubelet: the CSI driver's
//! `requiresRepublish: true` republishes the volume every minute or so,
//! and `fsGroupPolicy: File` re-applies the pod's `fsGroup` on every
//! republish — a `chown :fsGroup` and a `chmod g+rw` of every file,
//! the one being appended included.
//!
//! The kernel stores the size of every attribute reply in the inode's
//! `i_size` and positions each `O_APPEND` write there. A `setattr` (or a
//! `link`) answered with the committed row's size while the write session
//! held more, so the appender's next blocks overwrote blocks `write(2)` had
//! already acknowledged: the file ended up short by the gap, every call
//! and the descriptor's `close()` succeeding (22.7 MB of 2 GB on kind).
//!
//! Two legs, both nodes in turn: `b` (the holder's backup, a non-owner
//! whose publications are forwarded — the kind topology) and `a` (the
//! sequencer). Each appends for `APPEND_SETATTR_SECS` (8) seconds with an
//! attribute change every ~100 ms; then every acknowledged byte must be in
//! the file, in order, on the writer, on the other node, and on a fresh
//! third node reading through the bucket.

use super::eventually;
use super::m9::{cluster, unmount_all, wait_for_backup};
use crate::client::Client;
use anyhow::{bail, ensure, Context, Result};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::IntoRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BLOCK: usize = 64 * 1024;

/// Block `i` of the run seeded `seed`: its index up front, so a block
/// that landed on another block's place is told apart.
fn block(seed: u64, i: u64) -> Vec<u8> {
    let mut b = vec![(seed as u8) ^ (i as u8); BLOCK];
    b[..8].copy_from_slice(&i.to_le_bytes());
    b[8..16].copy_from_slice(&seed.to_le_bytes());
    b
}

fn secs() -> u64 {
    std::env::var("APPEND_SETATTR_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

fn cpath(path: &Path) -> std::ffi::CString {
    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("no NUL in a harness path")
}

/// What one leg's writer saw.
struct Appended {
    blocks: u64,
    setattrs: u64,
    fsyncs: u64,
}

/// One leg: append to `path` through one descriptor while other threads
/// `fsync` it from their own descriptors and change its attributes, for
/// `run_for`; the descriptor's `close()` must succeed.
fn append_under_setattr(path: &Path, seed: u64, run_for: Duration) -> Result<Appended> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {} O_APPEND", path.display()))?;
    let stop = Arc::new(AtomicBool::new(false));
    let setattrs = Arc::new(AtomicU64::new(0));
    let fsyncs = Arc::new(AtomicU64::new(0));
    // The kubelet's fsGroup pass (and a `ln`): attribute replies about
    // the inode while it is being appended to.
    let changer = {
        let (stop, setattrs, path) = (stop.clone(), setattrs.clone(), path.to_path_buf());
        std::thread::spawn(move || -> Result<()> {
            let c = cpath(&path);
            let link = path.with_extension("lnk");
            let gid = unsafe { libc::getgid() };
            let mut n = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let rc = match n % 4 {
                    0 => unsafe { libc::chown(c.as_ptr(), u32::MAX, gid) },
                    1 => unsafe { libc::chmod(c.as_ptr(), 0o664) },
                    2 => unsafe { libc::utimes(c.as_ptr(), std::ptr::null()) },
                    _ => match std::fs::hard_link(&path, &link) {
                        Ok(()) => std::fs::remove_file(&link).map(|()| 0).unwrap_or(-1),
                        Err(_) => -1,
                    },
                };
                if rc != 0 {
                    bail!(
                        "attribute change {} of {} failed: {}",
                        n % 4,
                        path.display(),
                        std::io::Error::last_os_error()
                    );
                }
                n += 1;
                setattrs.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok(())
        })
    };
    // `sync -d` from a second descriptor, every second.
    let syncer = {
        let (stop, fsyncs, path) = (stop.clone(), fsyncs.clone(), path.to_path_buf());
        std::thread::spawn(move || -> Result<()> {
            while !stop.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_secs(1));
                let f = std::fs::File::open(&path)?;
                f.sync_data()
                    .with_context(|| format!("fdatasync of {}", path.display()))?;
                fsyncs.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    };
    let started = Instant::now();
    let mut blocks = 0u64;
    let mut failed = None;
    while started.elapsed() < run_for {
        if let Err(e) = f.write_all(&block(seed, blocks)) {
            failed = Some(e);
            break;
        }
        blocks += 1;
    }
    stop.store(true, Ordering::SeqCst);
    let changed = changer.join().expect("changer thread");
    let synced = syncer.join().expect("syncer thread");
    if let Some(e) = failed {
        bail!("append {blocks} of {} failed: {e}", path.display());
    }
    // The descriptor's own close, checked (a `File` drop ignores it).
    if unsafe { libc::close(f.into_raw_fd()) } != 0 {
        bail!(
            "close() of the appending descriptor of {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    changed?;
    synced?;
    Ok(Appended {
        blocks,
        setattrs: setattrs.load(Ordering::SeqCst),
        fsyncs: fsyncs.load(Ordering::SeqCst),
    })
}

/// Every acknowledged block of `path`, in order, and nothing more: the
/// first block found elsewhere than its place is the failure.
fn verify(who: &str, path: &Path, seed: u64, blocks: u64) -> Result<()> {
    let want = blocks * BLOCK as u64;
    let size = std::fs::metadata(path)
        .with_context(|| format!("{who}: stat {}", path.display()))?
        .len();
    let mut f = std::fs::File::open(path)?;
    let mut buf = vec![0u8; BLOCK];
    for i in 0..blocks {
        let mut got = 0;
        while got < BLOCK {
            let n = f.read(&mut buf[got..])?;
            if n == 0 {
                break;
            }
            got += n;
        }
        if got < BLOCK || buf != block(seed, i) {
            let found = (got >= 8).then(|| u64::from_le_bytes(buf[..8].try_into().unwrap()));
            bail!(
                "{who}: {} is {size} bytes, {want} acknowledged ({} short); block {i} of \
                 {blocks} (offset {}) holds {}",
                path.display(),
                want as i64 - size as i64,
                i * BLOCK as u64,
                match found {
                    Some(j) if got == BLOCK => format!("block {j}"),
                    _ => format!("{got} bytes"),
                }
            );
        }
    }
    ensure!(
        size == want,
        "{who}: {} is {size} bytes, {want} acknowledged",
        path.display()
    );
    Ok(())
}

pub fn append_setattr_size(seed: u64) -> Result<()> {
    let scenario = "append-setattr-size";
    // Placement off: the lease stays on `a`, so `b`'s publications are
    // forwarded as on kind, where the controller's engine pod kept it.
    let (_env, root, mut clients, _) = cluster(
        scenario,
        &["a", "b"],
        &[("CONSTELLATION_LEASE_PLACEMENT", "off")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let backups = wait_for_backup(&clients[0], Duration::from_secs(60))?;
        eprintln!("    {scenario}: a holds the lease, backups {backups:?}");
        let run_for = Duration::from_secs(secs());
        let mut legs: Vec<(String, PathBuf, u64, u64)> = Vec::new();
        for (writer, leg_seed) in [(1usize, seed), (0usize, seed.wrapping_add(1))] {
            let c = &clients[writer];
            let name = format!("busy-{}", c.name);
            let path = c.mnt.join(&name);
            let got = append_under_setattr(&path, leg_seed, run_for)?;
            eprintln!(
                "    {scenario}: {} appended {} blocks ({} MiB) under {} attribute changes \
                 and {} fsyncs",
                c.name,
                got.blocks,
                (got.blocks * BLOCK as u64) >> 20,
                got.setattrs,
                got.fsyncs
            );
            ensure!(
                got.setattrs > 0 && got.blocks > 0,
                "{}: the leg did not run",
                c.name
            );
            verify(&format!("{} (writer)", c.name), &path, leg_seed, got.blocks)?;
            legs.push((name, path, leg_seed, got.blocks));
        }
        // The other node, once it follows.
        for (writer, (name, _, leg_seed, blocks)) in [(0usize, &legs[0]), (1usize, &legs[1])] {
            let other = &clients[writer];
            eventually(
                &format!("{} reads {name} whole", other.name),
                Duration::from_secs(60),
                || verify(&other.name, &other.mnt.join(name), *leg_seed, *blocks),
            )?;
        }
        // A fresh node, through the bucket.
        let (a_backend, a_endpoint) = (clients[0].backend.clone(), clients[0].endpoint.clone());
        let mut fresh = Client::new(root.path(), "c", &a_endpoint, &a_backend)?.with_own_node_key();
        fresh.mount()?;
        let checked = (|| {
            for (name, _, leg_seed, blocks) in &legs {
                eventually(
                    &format!("c reads {name} whole"),
                    Duration::from_secs(60),
                    || verify("c (fresh)", &fresh.mnt.join(name), *leg_seed, *blocks),
                )?;
            }
            Ok(())
        })();
        let _ = fresh.unmount();
        checked
    })();
    if result.is_err() {
        for c in &clients {
            eprintln!("--- {} log tail:\n{}", c.name, c.tail_log());
        }
    }
    unmount_all(&mut clients);
    result
}
