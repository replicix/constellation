//! Plan 30 §M5 phase 2 scenarios: the stale-base speculation rule on the
//! wire (`PeerMsg::MutateReply::base`).

use super::{eventually, ino_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use std::path::Path;
use std::time::{Duration, Instant};

/// A requester must not install a holder's accepted reply ahead of the
/// log when the holder evaluated the op on unshipped state the requester
/// has not seen (plan 30 M5 phase 1 found it; phase 2 puts `base` on the
/// wire and waits for the log instead).
///
/// The shape, forced with `CONSTELLATION_FAULT_HOLD_SYNC_FILE` rather than
/// raced: `f1` and `f2` exist everywhere; A (holder) and B (requester)
/// have their sync rounds held, so A ships nothing. A unlinks `f2`
/// (journaled, unshipped). B, whose replica still has both files, renames
/// `f1` over `f2`: a rename onto an existing name passes B's own
/// validation, A executes it (`f2` is free there) and replies with `base`
/// = its unshipped position, because the unlink overlaps the rename's
/// keys.
///
/// Pre-fix (main before this milestone), B installs the rename as a
/// shadow onto a replica where `f2` is still the old inode; the log then
/// applies A's `Unlink(f2)` on top — removing the entry that is now the
/// renamed `f1` — and the rename record finds no `f1` left to move, so B
/// ends up without the file A and C have: the rename divergence.
/// Post-fix, B's rename blocks in `AwaitingLog` until the holds lift and
/// the log carries the unlink and the rename in order, and A, B and C
/// agree `f2` is `f1`'s inode with `f1`'s content and `f1` is gone.
///
/// No backups (`CONSTELLATION_BACKUPS=0` on every node): with one, A's
/// tenure is backed and its pre-S3 stream carries the unlink to every
/// subscriber, B included, which the sync hold does not stop — B then
/// saw the unlink before its rename and the shape never formed (the
/// scenario failed with "B already sees the unlink" since plan 30 §M9).
pub fn stale_base_rename_divergence(_seed: u64) -> Result<()> {
    let (env, root) = setup("stale-base-rename-divergence")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/stale-base-{}", ts());
    let hold_a = root.path().join("hold-a");
    let hold_b = root.path().join("hold-b");
    let mk = |name: &str, hold: Option<&Path>| -> Result<Client> {
        let mut c = Client::new(root.path(), name, &env.endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_BACKUPS", "0");
        if let Some(hold) = hold {
            c = c.with_env(
                "CONSTELLATION_FAULT_HOLD_SYNC_FILE",
                hold.to_str().context("hold path")?,
            );
        }
        Ok(c)
    };
    let mut a = mk("a", Some(&hold_a))?;
    let mut b = mk("b", Some(&hold_b))?;
    let mut c = mk("c", None)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    wait_for_p2p(&[&a, &b, &c])?;

    // A holds; `f1` and `f2` exist everywhere.
    std::fs::write(a.mnt.join("f1"), b"one")?;
    std::fs::write(a.mnt.join("f2"), b"two")?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        let lease = a.control_status()?["lease"].clone();
        anyhow::ensure!(lease["held"] == true, "A does not hold: {lease}");
        Ok(())
    })?;
    eventually(
        "`f1` and `f2` visible on B and C",
        Duration::from_secs(20),
        || {
            for node in [&b, &c] {
                anyhow::ensure!(std::fs::read(node.mnt.join("f1"))? == b"one", "f1 stale");
                anyhow::ensure!(std::fs::read(node.mnt.join("f2"))? == b"two", "f2 stale");
            }
            Ok(())
        },
    )?;
    let f1_ino = ino_of(&a.mnt.join("f1")).context("f1 missing on A")?;

    // Hold A's and B's sync rounds. Each daemon writes `<hold>.held` from
    // the first round that observes the hold; rounds run one at a time,
    // so from then on nothing A journals ships until the hold lifts.
    std::fs::write(&hold_a, b"hold")?;
    std::fs::write(&hold_b, b"hold")?;
    let held_a = root.path().join("hold-a.held");
    let held_b = root.path().join("hold-b.held");
    eventually(
        "A's and B's rounds observe the hold",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(held_a.is_file(), "A has not observed the hold");
            anyhow::ensure!(held_b.is_file(), "B has not observed the hold");
            Ok(())
        },
    )?;

    let result = (|| -> Result<()> {
        // A's unlink stays in its journal: B and C keep seeing `f2`.
        std::fs::remove_file(a.mnt.join("f2")).context("A's unlink")?;
        std::thread::sleep(Duration::from_secs(1));
        anyhow::ensure!(
            b.mnt.join("f2").is_file(),
            "B already sees the unlink: A shipped while held"
        );

        // B's rename of `f1` over `f2` forwards to A, where `f2` is gone.
        let rename = {
            let (from, to) = (b.mnt.join("f1"), b.mnt.join("f2"));
            std::thread::spawn(move || -> std::io::Result<Duration> {
                let started = Instant::now();
                std::fs::rename(&from, &to).map(|_| started.elapsed())
            })
        };
        // Post-fix, the rename waits for the log (A's reply names a base
        // B has not applied); pre-fix it returns at once on a shadow.
        std::thread::sleep(Duration::from_secs(3));
        let returned_while_held = rename.is_finished();
        let b_spec = b.control_status()?["speculation"].clone();
        eprintln!(
            "    stale-base-rename-divergence: B's rename {} while the log was held; B speculation: {b_spec}",
            if returned_while_held { "RETURNED" } else { "is waiting" }
        );

        // Lift the holds: A ships the unlink and the rename, B tails.
        std::fs::remove_file(&hold_a)?;
        std::fs::remove_file(&hold_b)?;
        let deadline = Instant::now() + Duration::from_secs(60);
        while !rename.is_finished() {
            anyhow::ensure!(
                Instant::now() < deadline,
                "B's rename did not return within 60 s of lifting the hold"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let elapsed = rename
            .join()
            .map_err(|_| anyhow::anyhow!("rename thread panicked"))?
            .context("B's rename of `f1` over `f2` must succeed")?;
        eprintln!("    stale-base-rename-divergence: B's rename returned OK after {elapsed:?}");

        // Convergence: `f2` is `f1`'s inode with "one" everywhere, `f1`
        // is gone everywhere.
        eventually(
            "A, B and C agree on `f1` and `f2`",
            Duration::from_secs(30),
            || {
                for (name, node) in [("A", &a), ("B", &b), ("C", &c)] {
                    let f2_ino = ino_of(&node.mnt.join("f2")).with_context(|| {
                        format!("`f2` missing on {name}: the unlink removed the renamed entry")
                    })?;
                    anyhow::ensure!(
                        f2_ino == f1_ino,
                        "`f2` on {name} is inode {f2_ino}, not the renamed `f1` {f1_ino}"
                    );
                    anyhow::ensure!(
                        std::fs::read(node.mnt.join("f2"))? == b"one",
                        "`f2` on {name} does not carry `f1`'s content"
                    );
                    anyhow::ensure!(!node.mnt.join("f1").exists(), "`f1` still exists on {name}");
                }
                Ok(())
            },
        )?;
        anyhow::ensure!(
            !returned_while_held,
            "B's rename returned while A's unlink was unshipped: B installed speculation on a \
             base it had not applied (plan 30 M5's stale-base rule)"
        );
        Ok(())
    })();

    let _ = std::fs::remove_file(&hold_a);
    let _ = std::fs::remove_file(&hold_b);
    c.unmount()?;
    b.unmount()?;
    a.unmount()?;
    result
}
