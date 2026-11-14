//! Plan 46 M1.2 step 15: `two-subtrees-one-stream-rename`. Two nodes: `a`
//! holds the root, `b` is its backup and the delegate of both `/d1` and
//! `/d2`: one grant, one stream, two subtrees (a node has one stream, the
//! folded subtree set). `b` renames files between the two subtrees, round
//! after round, each rename a row of the one stream (no cross-subtree
//! hazard: the stream owns both parents); `a` is `kill -9`ed in the middle
//! and `b` takes the root over by seal. Every rename `b` acknowledged stands
//! afterwards: each file is in the subtree its last acknowledged rename
//! left it in, once, byte-exact, on both nodes (`a` rejoins), with no
//! conflict copy, and no rename failed across the failover.

use super::m11::{deleg_of, delegate, dump_logs_on_failure, node_id, unmount_all, wait_installed};
use super::m9::wait_for_backup;
use super::{eventually, lease_of};
use crate::client::Client;
use anyhow::{ensure, Context, Result};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Files renamed back and forth between `/d1` and `/d2`.
const FILES: usize = 24;
/// Acknowledged renames before the root is killed, and after `b` holds it.
const RENAMES_BEFORE: usize = 60;
const RENAMES_AFTER: usize = 60;

/// Files that keep renaming after the root is killed: the others stay where
/// their last pre-kill acknowledged rename left them, so the final check
/// sees those positions directly.
const MOVING_AFTER: usize = FILES / 2;

/// The ledger's lock. It holds plain data, so a renamer's panic (reported
/// at its join) is no reason to panic here too.
fn locked(l: &Mutex<Ledger>) -> MutexGuard<'_, Ledger> {
    l.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the renamer knows: where each file stands by its last acknowledged
/// rename (0: `/d1`, 1: `/d2`), the rename in flight (file, target) and
/// the first failure.
struct Ledger {
    /// The renamer cycles through files `0..cycle`.
    cycle: usize,
    loc: Vec<u8>,
    pending: Option<(usize, u8)>,
    acked: usize,
    err: Option<String>,
}

fn dir_of(side: u8) -> &'static str {
    if side == 0 {
        "d1"
    } else {
        "d2"
    }
}

fn content_of(i: usize) -> Vec<u8> {
    format!("two-subtrees-one-stream-rename file {i}").into_bytes()
}

/// A thread renaming file after file to the other subtree until `stop`.
fn renamer(
    mnt: PathBuf,
    stop: Arc<AtomicBool>,
    ledger: Arc<Mutex<Ledger>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut k = 0usize;
        while !stop.load(Ordering::Relaxed) {
            let (i, from, to) = {
                let mut l = locked(&ledger);
                let i = k % l.cycle;
                k += 1;
                let from = l.loc[i];
                l.pending = Some((i, 1 - from));
                (i, from, 1 - from)
            };
            let r = std::fs::rename(
                mnt.join(dir_of(from)).join(format!("f{i}")),
                mnt.join(dir_of(to)).join(format!("f{i}")),
            );
            let mut l = locked(&ledger);
            match r {
                Ok(()) => {
                    l.loc[i] = to;
                    l.pending = None;
                    l.acked += 1;
                }
                Err(e) => {
                    l.err = Some(format!("rename of f{i} to {}: {e}", dir_of(to)));
                    return;
                }
            }
        }
    })
}

/// `b` holds exactly one live generation and it covers both subtrees.
pub(super) fn one_stream(b: &Client) -> Result<()> {
    let d = deleg_of(b)?;
    let live: Vec<&serde_json::Value> = d["mine"]
        .as_array()
        .map(|m| m.iter().filter(|g| g[3] == false).collect())
        .unwrap_or_default();
    ensure!(
        live.len() == 1 && live[0][0].as_array().is_some_and(|dirs| dirs.len() == 2),
        "b does not hold one generation over both subtrees: {}",
        d["mine"]
    );
    Ok(())
}

/// The rows `node`'s takeover gate re-appended from its own delegate stream
/// (`rows` of the info line "re-appended this node's own delegate rows after
/// the predecessor's tail"), summed over the log.
fn reappended_rows(node: &Client) -> u64 {
    node.log_text()
        .lines()
        .filter(|l| l.contains("re-appended this node's own delegate rows"))
        .filter_map(|l| {
            crate::verify::field(&crate::verify::strip_ansi(l), "rows")
                .and_then(|v| v.parse::<u64>().ok())
        })
        .sum()
}

/// `b`'s streamed index of its one live generation and the root's cursor of
/// it (`status.delegation`: `mine` and `gens`).
fn stream_counters(b: &Client, root: &Client) -> Result<(u64, u64)> {
    let mine = deleg_of(b)?;
    let streamed = mine["mine"]
        .as_array()
        .and_then(|m| m.iter().find(|g| g[3] == false))
        .and_then(|g| g[4].as_u64())
        .context("b reports no live generation")?;
    let gens = deleg_of(root)?;
    let cursor = gens["gens"]
        .as_array()
        .and_then(|m| m.iter().find(|g| g[6] == false))
        .and_then(|g| g[3].as_u64())
        .context("the root reports no live generation of b's")?;
    Ok((streamed, cursor))
}

/// `two-subtrees-one-stream-rename`: see the module doc.
pub fn two_subtrees_one_stream_rename(_seed: u64) -> Result<()> {
    let name = "two-subtrees-one-stream-rename";
    let (_env, _root, mut clients) = super::delegroot::delegated_pair_with(name, true, &[])?;
    let result = (|| -> Result<()> {
        let b_id = node_id(&clients[1])?;
        for d in ["d1", "d2"] {
            std::fs::create_dir(clients[0].mnt.join(d))?;
        }
        eventually("dirs visible on b", Duration::from_secs(20), || {
            ensure!(clients[1].mnt.join("d1").is_dir() && clients[1].mnt.join("d2").is_dir());
            Ok(())
        })?;
        delegate(&clients[0], "/d1", b_id)?;
        delegate(&clients[0], "/d2", b_id)?;
        let g1 = wait_installed(&clients[1], "/d1", Duration::from_secs(20))?;
        let g2 = wait_installed(&clients[1], "/d2", Duration::from_secs(20))?;
        ensure!(g1 == g2, "/d1 is generation {g1}, /d2 generation {g2}");
        eventually(
            "one stream over both subtrees",
            Duration::from_secs(20),
            || one_stream(&clients[1]),
        )?;
        // `b` must be the root's listed backup, or the takeover is the TTL
        // one and the scenario another.
        let backups = wait_for_backup(&clients[0], Duration::from_secs(30))?;
        ensure!(
            backups == vec![b_id],
            "a backs up to {backups:?}, not b ({b_id})"
        );
        eprintln!("    {name}: b holds /d1 and /d2 as generation {g1}, a's backup");

        for i in 0..FILES {
            let mut f = std::fs::File::create(clients[1].mnt.join("d1").join(format!("f{i}")))?;
            f.write_all(&content_of(i))?;
            f.sync_all()?;
        }
        let epoch = lease_of(&clients[0])?["epoch"]
            .as_u64()
            .context("a's lease reports no epoch")?;
        let stop = Arc::new(AtomicBool::new(false));
        let ledger = Arc::new(Mutex::new(Ledger {
            cycle: FILES,
            loc: vec![0; FILES],
            pending: None,
            acked: 0,
            err: None,
        }));
        let writer = renamer(clients[1].mnt.clone(), stop.clone(), ledger.clone());
        let acked = || locked(&ledger).acked;
        let failed = || locked(&ledger).err.clone();
        let wait_acked = |what: &str, n: usize| {
            eventually(what, Duration::from_secs(90), || {
                if let Some(e) = failed() {
                    anyhow::bail!("{e}");
                }
                ensure!(acked() >= n, "{} acknowledged renames", acked());
                Ok(())
            })
        };
        let outcome = (|| -> Result<()> {
            wait_acked("renames under the grant", RENAMES_BEFORE)?;
            one_stream(&clients[1])?;
            let at_kill = acked();
            let (streamed, cursor) = stream_counters(&clients[1], &clients[0])?;
            eprintln!(
                "    {name}: before the kill b streamed through {streamed}, a's cursor for b's stream is {cursor}"
            );
            // From here only the first half of the files is renamed: the
            // rest keep their pre-kill positions to the end.
            locked(&ledger).cycle = MOVING_AFTER;
            let killed = Instant::now();
            clients[0].kill9()?;
            eventually(
                "b holds the root",
                crate::verify::hang_bound().max(Duration::from_secs(90)),
                || {
                    if let Some(e) = failed() {
                        anyhow::bail!("{e}");
                    }
                    let l = lease_of(&clients[1])?;
                    ensure!(
                        l["held"] == true
                            && l["epoch"].as_u64().context("b's lease reports no epoch")? > epoch,
                        "b's lease: {l}"
                    );
                    Ok(())
                },
            )?;
            eprintln!(
                "    {name}: b held the root {:?} after the kill ({} renames acknowledged by then)",
                killed.elapsed(),
                acked()
            );
            // What the kill left behind, for attribution: the rows of its
            // own stream `b` re-appended at the takeover. Not asserted: `b`'s
            // acknowledgement waits for the root's append, so no acknowledged
            // row is missing from `a`'s log (the pre-kill counters above show
            // `a`'s cursor never behind `b`'s streamed index), and the tail
            // this would count is the case of a delegate with a backup group
            // of its own. A rename in flight at the kill is what crosses the
            // takeover here (the `pending` check below).
            eprintln!(
                "    {name}: the takeover re-appended {} own delegate row(s)",
                reappended_rows(&clients[1])
            );
            let at = acked();
            wait_acked("renames on the new root", at + RENAMES_AFTER)?;
            eprintln!(
                "    {name}: {at_kill} renames before the kill, {} in all",
                acked()
            );
            Ok(())
        })();
        stop.store(true, Ordering::Relaxed);
        let joined = writer.join();
        outcome?;
        joined.map_err(|_| anyhow::anyhow!("the renamer thread panicked"))?;
        let l = locked(&ledger);
        ensure!(l.err.is_none(), "a rename failed: {:?}", l.err);
        ensure!(
            l.pending.is_none(),
            "a rename never returned: {:?}",
            l.pending
        );

        // `a` returns and rejoins as a plain member.
        clients[0].mount()?;
        let check = |c: &Client| -> Result<()> {
            for (i, side) in l.loc.iter().enumerate() {
                let want = c.mnt.join(dir_of(*side)).join(format!("f{i}"));
                let other = c.mnt.join(dir_of(1 - side)).join(format!("f{i}"));
                let got = std::fs::read(&want)
                    .with_context(|| format!("f{i} is not in {} on {}", dir_of(*side), c.name))?;
                ensure!(got == content_of(i), "f{i} on {} reads {got:?}", c.name);
                ensure!(
                    !other.exists(),
                    "f{i} is in both subtrees on {} (last acknowledged: {})",
                    c.name,
                    dir_of(*side)
                );
            }
            let mut names = 0;
            for d in ["d1", "d2"] {
                names += std::fs::read_dir(c.mnt.join(d))?.count();
            }
            ensure!(
                names == FILES,
                "{} lists {names} names, not {FILES}",
                c.name
            );
            Ok(())
        };
        eventually(
            "every file in its place on both nodes",
            Duration::from_secs(120),
            || {
                check(&clients[0])?;
                check(&clients[1])
            },
        )?;
        super::ensure_no_conflicts(&[&clients[0], &clients[1]])?;
        Ok(())
    })();
    dump_logs_on_failure(name, &clients, &result);
    unmount_all(&mut clients);
    result
}
