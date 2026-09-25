//! Plan 30 §M14 scenarios: cross-node `flock`/`fcntl` under `--locks
//! cluster` — exclusion across nodes, SQLite with two concurrent writers,
//! a partitioned lock holder fenced with `EIO` while the other node gets
//! the lock only after the lease's expiry plus margin, failover with a
//! lock held, and the latency measurements the plan asks for.
//!
//! The harness process is the application: it opens files on the mounts
//! and calls `flock(2)`/`fcntl(2)` on them, so what the kernel sends the
//! filesystem is exactly what an application would send.

use super::m8::dist;
use super::m9::{cluster, node_id, wait_for_backup, wait_holds};
use super::{eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn locks_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["locks"].clone())
}

fn print_locks(scenario: &str, c: &Client) {
    match locks_of(c) {
        Ok(v) => eprintln!("    {scenario}: {} locks: {v}", c.name),
        Err(e) => eprintln!("    {scenario}: {} locks: (no status: {e})", c.name),
    }
}

fn open_rw(path: &Path) -> Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))
}

/// `flock(2)`; `Err(errno)` on failure.
fn flock(f: &File, op: libc::c_int) -> std::result::Result<(), i32> {
    if unsafe { libc::flock(f.as_raw_fd(), op) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
    }
}

fn flock_timed(f: &File, op: libc::c_int) -> (std::result::Result<(), i32>, Duration) {
    let t = Instant::now();
    let r = flock(f, op);
    (r, t.elapsed())
}

/// `fcntl(F_SETLK/F_SETLKW)` on `[start, start+len)`.
fn fcntl_lock(
    f: &File,
    cmd: libc::c_int,
    typ: i16,
    start: i64,
    len: i64,
) -> std::result::Result<(), i32> {
    let mut l: libc::flock = unsafe { std::mem::zeroed() };
    l.l_type = typ;
    l.l_whence = libc::SEEK_SET as i16;
    l.l_start = start;
    l.l_len = len;
    if unsafe { libc::fcntl(f.as_raw_fd(), cmd, &l) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
    }
}

/// `fcntl(F_GETLK)`: the conflicting lock's type (`F_UNLCK` for none).
fn fcntl_getlk(f: &File, typ: i16, start: i64, len: i64) -> Result<i16> {
    let mut l: libc::flock = unsafe { std::mem::zeroed() };
    l.l_type = typ;
    l.l_whence = libc::SEEK_SET as i16;
    l.l_start = start;
    l.l_len = len;
    if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_GETLK, &mut l) } != 0 {
        bail!("F_GETLK: {}", std::io::Error::last_os_error());
    }
    Ok(l.l_type)
}

fn write_at(f: &mut File, off: u64, data: &[u8]) -> std::io::Result<()> {
    f.seek(SeekFrom::Start(off))?;
    f.write_all(data)?;
    f.flush()
}

fn read_at(f: &mut File, off: u64, len: usize) -> std::io::Result<Vec<u8>> {
    f.seek(SeekFrom::Start(off))?;
    let mut buf = vec![0u8; len];
    let n = f.read(&mut buf)?;
    buf.truncate(n);
    Ok(buf)
}

/// Take `op` on `path` in a thread; returns when it is held, with how long
/// it took, and keeps the file (and the lock) until the receiver is
/// dropped or `release` is signalled.
struct HeldLock {
    took: Duration,
    release: Arc<AtomicBool>,
    done: std::sync::mpsc::Receiver<()>,
}

impl HeldLock {
    fn release(self) {
        self.release.store(true, Ordering::SeqCst);
        let _ = self.done.recv_timeout(Duration::from_secs(30));
    }
}

fn hold_flock(path: PathBuf, op: libc::c_int, deadline: Duration) -> Result<HeldLock> {
    let release = Arc::new(AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::channel();
    let (dtx, drx) = std::sync::mpsc::channel();
    let rel = release.clone();
    std::thread::spawn(move || {
        let f = match open_rw(&path) {
            Ok(f) => f,
            Err(e) => {
                let _ = tx.send(Err(e));
                return;
            }
        };
        let (r, took) = flock_timed(&f, op);
        let _ = tx.send(
            r.map(|_| took)
                .map_err(|e| anyhow::anyhow!("flock errno {e}")),
        );
        while !rel.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = flock(&f, libc::LOCK_UN);
        drop(f);
        let _ = dtx.send(());
    });
    let took = rx
        .recv_timeout(deadline)
        .context("the lock thread did not answer in time")??;
    Ok(HeldLock {
        took,
        release,
        done: drx,
    })
}

// ------------------------------------------------------------------ flock-cross-node

/// Two nodes: an exclusive `flock` on one excludes the other (non-blocking
/// `EWOULDBLOCK`, blocking waits until the unlock), shared locks coexist,
/// `fcntl` ranges conflict across nodes and `F_GETLK` sees the remote
/// holder, writes under the lock are visible to the next holder — and,
/// for the record, `--locks local` (today's behaviour) lets both nodes
/// hold "exclusive" locks at once.
pub fn flock_cross_node(_seed: u64) -> Result<()> {
    // Each phase owns its environment (one docker prefix at a time).
    flock_cross_node_cluster()?;
    flock_cross_node_local()
}

fn flock_cross_node_cluster() -> Result<()> {
    const NAME: &str = "flock-cross-node";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b"], &[], 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let fa = a.mnt.join("f");
        let fb = b.mnt.join("f");

        // Exclusive on A (the holder: local, no message).
        let held_a = hold_flock(fa.clone(), libc::LOCK_EX, Duration::from_secs(20))?;
        eprintln!("    {NAME}: A took LOCK_EX in {:?}", held_a.took);
        // B: non-blocking must fail, blocking must wait for A's unlock.
        let fb_file = open_rw(&fb)?;
        let (r, took) = flock_timed(&fb_file, libc::LOCK_EX | libc::LOCK_NB);
        anyhow::ensure!(
            r == Err(libc::EWOULDBLOCK),
            "B's LOCK_EX|LOCK_NB while A holds: {r:?} (took {took:?})"
        );
        eprintln!("    {NAME}: B's non-blocking LOCK_EX refused (EWOULDBLOCK) in {took:?}");
        let started = Instant::now();
        let waiter = std::thread::spawn({
            let fb = fb.clone();
            move || -> Result<Duration> {
                let f = open_rw(&fb)?;
                let (r, took) = flock_timed(&f, libc::LOCK_EX);
                r.map_err(|e| anyhow::anyhow!("errno {e}"))?;
                // Hold it a moment so A can observe the exclusion.
                std::thread::sleep(Duration::from_millis(300));
                let _ = flock(&f, libc::LOCK_UN);
                Ok(took)
            }
        });
        std::thread::sleep(Duration::from_millis(1500));
        anyhow::ensure!(
            !waiter.is_finished(),
            "B's blocking LOCK_EX returned while A holds"
        );
        held_a.release();
        let waited = waiter.join().expect("waiter")?;
        eprintln!(
            "    {NAME}: B's blocking LOCK_EX granted {waited:?} after its request, {:?} after A's unlock",
            started.elapsed().saturating_sub(Duration::from_millis(1500))
        );
        anyhow::ensure!(
            waited >= Duration::from_millis(1400),
            "B was granted too early: {waited:?}"
        );

        // Shared pair.
        let sa = hold_flock(fa.clone(), libc::LOCK_SH, Duration::from_secs(20))?;
        let sb = hold_flock(fb.clone(), libc::LOCK_SH, Duration::from_secs(20))?;
        eprintln!(
            "    {NAME}: shared pair held: A {:?}, B {:?}",
            sa.took, sb.took
        );
        // An exclusive request from A now conflicts with B's shared grant.
        let f2 = open_rw(&fa)?;
        let (r, _) = flock_timed(&f2, libc::LOCK_EX | libc::LOCK_NB);
        anyhow::ensure!(
            r == Err(libc::EWOULDBLOCK),
            "A's upgrade with B shared: {r:?}"
        );
        sb.release();
        sa.release();

        // fcntl ranges: A writes [0, 100), B's overlapping write lock is
        // refused, its disjoint one is fine (both on B: the node-level
        // grant is per file, so B's disjoint range needs A's grant to be
        // shared or gone — it is exclusive: refused too, by design; the
        // POSIX split holds within a node).
        let fa2 = open_rw(&fa)?;
        // Blocking: B's release of its shared grant may still be in flight.
        fcntl_lock(&fa2, libc::F_SETLKW, libc::F_WRLCK as i16, 0, 100)
            .map_err(|e| anyhow::anyhow!("A F_SETLK: errno {e}"))?;
        let fb2 = open_rw(&fb)?;
        let r = fcntl_lock(&fb2, libc::F_SETLK, libc::F_WRLCK as i16, 50, 10);
        anyhow::ensure!(
            r == Err(libc::EAGAIN) || r == Err(libc::EACCES),
            "B's overlapping F_SETLK: {r:?}"
        );
        let t = fcntl_getlk(&fb2, libc::F_WRLCK as i16, 50, 10)?;
        anyhow::ensure!(
            t == libc::F_WRLCK as i16,
            "B's F_GETLK saw {t}, not F_WRLCK"
        );
        eprintln!("    {NAME}: fcntl: B's overlapping write lock refused; F_GETLK reports the remote write lock");
        // Data under the lock: A writes, unlocks; B locks and reads.
        let mut wa = open_rw(&fa)?;
        write_at(&mut wa, 0, b"under-lock-A")?;
        wa.sync_all()?;
        fcntl_lock(&fa2, libc::F_SETLK, libc::F_UNLCK as i16, 0, 100)
            .map_err(|e| anyhow::anyhow!("A F_UNLCK: errno {e}"))?;
        fcntl_lock(&fb2, libc::F_SETLKW, libc::F_WRLCK as i16, 0, 100)
            .map_err(|e| anyhow::anyhow!("B F_SETLKW: errno {e}"))?;
        let mut rb = open_rw(&fb)?;
        let got = read_at(&mut rb, 0, 12)?;
        anyhow::ensure!(
            got == b"under-lock-A",
            "B under its lock read {:?}",
            String::from_utf8_lossy(&got)
        );
        eprintln!(
            "    {NAME}: B's lock saw A's write made under A's lock (lock-to-unlock coherence)"
        );
        fcntl_lock(&fb2, libc::F_SETLK, libc::F_UNLCK as i16, 0, 100).ok();
        for c in &clients {
            print_locks(NAME, c);
        }
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// For the record: `--locks local` is node-local locking.
fn flock_cross_node_local() -> Result<()> {
    const NAME: &str = "flock-cross-node";
    let (_env, _root, mut clients, _) = cluster(
        "flock-cross-node-local",
        &["a", "b"],
        &[("CONSTELLATION_LOCKS", "local")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let fa = open_rw(&clients[0].mnt.join("f"))?;
        let fb = open_rw(&clients[1].mnt.join("f"))?;
        flock(&fa, libc::LOCK_EX | libc::LOCK_NB).map_err(|e| anyhow::anyhow!("errno {e}"))?;
        let r = flock(&fb, libc::LOCK_EX | libc::LOCK_NB);
        anyhow::ensure!(
            r.is_ok(),
            "--locks local: B's LOCK_EX while A holds should succeed (node-local): {r:?}"
        );
        eprintln!("    {NAME}: --locks local: both nodes hold LOCK_EX at once (today's behaviour, documented)");
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

// ------------------------------------------------------------------ sqlite-two-nodes

/// Concurrent writers on one SQLite database from two nodes (rollback
/// journal, `fcntl` locks, `busy_timeout`), then `PRAGMA integrity_check`
/// on both and every committed row present.
pub fn sqlite_two_nodes(_seed: u64) -> Result<()> {
    const NAME: &str = "sqlite-two-nodes";
    const TXNS: usize = 40;
    let (_env, _root, mut clients, _) =
        cluster(NAME, &["a", "b"], &[("CONSTELLATION_CTO", "strict")], 0)?;
    let result = (|| -> Result<()> {
        let db_a = clients[0].mnt.join("app.db");
        let db_b = clients[1].mnt.join("app.db");
        sqlite(
            &db_a,
            "CREATE TABLE t(node TEXT, i INTEGER, PRIMARY KEY(node, i));",
        )?;
        eventually("app.db visible on B", Duration::from_secs(30), || {
            anyhow::ensure!(db_b.exists(), "no app.db on B");
            Ok(())
        })?;
        let started = Instant::now();
        let mut threads = Vec::new();
        for (who, db) in [("a", db_a.clone()), ("b", db_b.clone())] {
            threads.push(std::thread::spawn(
                move || -> Result<(usize, Vec<Duration>)> {
                    let mut ok = 0;
                    let mut lat = Vec::new();
                    for i in 0..TXNS {
                        let t = Instant::now();
                        sqlite(
                            &db,
                            &format!("INSERT INTO t(node, i) VALUES('{who}', {i});"),
                        )
                        .with_context(|| format!("{who}: insert {i}"))?;
                        lat.push(t.elapsed());
                        ok += 1;
                    }
                    Ok((ok, lat))
                },
            ));
        }
        let mut committed = 0;
        let mut lat = Vec::new();
        for t in threads {
            let (n, l) = t.join().expect("writer")?;
            committed += n;
            lat.extend(l);
        }
        eprintln!(
            "    {NAME}: {committed} transactions committed from two nodes in {:?}; per-txn {}",
            started.elapsed(),
            dist(lat)
        );
        for (c, db) in clients.iter().zip([&db_a, &db_b]) {
            let ic = sqlite_query(db, "PRAGMA integrity_check;")?;
            anyhow::ensure!(ic.trim() == "ok", "{}: integrity_check: {ic}", c.name);
            let count: usize = sqlite_query(db, "SELECT count(*) FROM t;")?
                .trim()
                .parse()?;
            anyhow::ensure!(
                count == committed,
                "{}: {count} rows, {committed} committed (lost commits)",
                c.name
            );
            eprintln!("    {NAME}: {}: integrity_check ok, {count} rows", c.name);
            print_locks(NAME, c);
        }
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

fn sqlite(db: &Path, sql: &str) -> Result<()> {
    let out = std::process::Command::new("sqlite3")
        .arg("-cmd")
        .arg(".timeout 60000")
        .arg(db)
        .arg(sql)
        .output()
        .context("running sqlite3")?;
    anyhow::ensure!(
        out.status.success(),
        "sqlite3 {}: {}{}",
        sql,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

fn sqlite_query(db: &Path, sql: &str) -> Result<String> {
    let out = std::process::Command::new("sqlite3")
        .arg("-cmd")
        .arg(".timeout 60000")
        .arg(db)
        .arg(sql)
        .output()
        .context("running sqlite3")?;
    anyhow::ensure!(
        out.status.success(),
        "sqlite3 {}: {}",
        sql,
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

// ------------------------------------------------------------------ lock-holder-partitioned

/// Three nodes; B holds an exclusive lock and is cut off from the owner
/// (P2P denied both ways): its renewals fail, its writes under the lock
/// get `EIO` once its grant lapses, and C — waiting for the lock — is
/// granted only after the owner outwaited B's grant (ttl + margin), never
/// before B was fenced.
pub fn lock_holder_partitioned(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-holder-partitioned";
    const LOCK_TTL_MS: u64 = 3_000;
    let ttl = LOCK_TTL_MS.to_string();
    let (_env, root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[
            ("CONSTELLATION_LOCK_TTL_MS", &ttl),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let a_id = node_id(a)?;
        let b_id = node_id(b)?;
        let held = hold_flock(b.mnt.join("f"), libc::LOCK_EX, Duration::from_secs(20))?;
        eprintln!("    {NAME}: B took LOCK_EX in {:?}", held.took);
        let mut wb = open_rw(&b.mnt.join("f"))?;
        write_at(&mut wb, 0, b"B1")?;
        // Cut B from the owner (A), both directions.
        let cut = Instant::now();
        std::fs::write(
            super::m9::c_deny_path(root.path(), &a.name),
            format!("{b_id}\n"),
        )?;
        std::fs::write(
            super::m9::c_deny_path(root.path(), &b.name),
            format!("{a_id}\n"),
        )?;
        eprintln!("    {NAME}: cut B <-> A");
        // C asks (blocking) for the lock.
        let granted_at = Arc::new(std::sync::Mutex::new(None::<Instant>));
        let waiter = std::thread::spawn({
            let fc = c.mnt.join("f");
            let granted_at = granted_at.clone();
            move || -> Result<()> {
                let f = open_rw(&fc)?;
                flock(&f, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("errno {e}"))?;
                *granted_at.lock().unwrap() = Some(Instant::now());
                std::thread::sleep(Duration::from_millis(500));
                let _ = flock(&f, libc::LOCK_UN);
                Ok(())
            }
        });
        // B keeps writing under its lock: EIO once fenced.
        let mut first_eio = None;
        let deadline = Instant::now() + Duration::from_millis(LOCK_TTL_MS * 3 + 5_000);
        while Instant::now() < deadline {
            match write_at(&mut wb, 0, b"B2") {
                Ok(()) => {}
                Err(e) if e.raw_os_error() == Some(libc::EIO) => {
                    first_eio.get_or_insert(Instant::now());
                }
                Err(e) => bail!("B's write under its lock: {e}"),
            }
            if first_eio.is_some() && granted_at.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let fenced = first_eio.context("B was never fenced (no EIO) within the deadline")?;
        let granted = granted_at
            .lock()
            .unwrap()
            .context("C was never granted the lock within the deadline")?;
        eprintln!(
            "    {NAME}: B fenced (EIO) {:?} after the cut; C granted {:?} after the cut",
            fenced - cut,
            granted - cut
        );
        anyhow::ensure!(fenced <= granted, "C was granted before B was fenced");
        anyhow::ensure!(
            granted - cut >= Duration::from_millis(LOCK_TTL_MS / 2),
            "C granted too early: {:?}",
            granted - cut
        );
        anyhow::ensure!(
            granted - cut <= Duration::from_millis(LOCK_TTL_MS + 1_000 + 4_000),
            "C granted too late: {:?}",
            granted - cut
        );
        waiter.join().expect("waiter")?;
        // Heal; B's application unlocks (its grant is long gone), and can
        // lock again.
        let _ = std::fs::remove_file(super::m9::c_deny_path(root.path(), &a.name));
        let _ = std::fs::remove_file(super::m9::c_deny_path(root.path(), &b.name));
        held.release();
        let again = hold_flock(b.mnt.join("f"), libc::LOCK_EX, Duration::from_secs(30))?;
        eprintln!(
            "    {NAME}: after the heal B locked again in {:?}",
            again.took
        );
        write_at(&mut wb, 0, b"B3").context("B's write after re-locking")?;
        again.release();
        for c in &clients {
            print_locks(NAME, c);
        }
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

// ------------------------------------------------------------------ lock-failover

/// Three nodes with an M9 backup; B holds an exclusive lock and writes
/// under it while the holder is killed. The backup takes over by seal; B's
/// renewals reclaim (or the mirror carries) its grant so its writes never
/// see `EIO`; C's non-blocking attempts are refused throughout, and C is
/// granted once B unlocks.
pub fn lock_failover(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-failover";
    // Long enough that B's window outlives the failover.
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c", "d"],
        &[
            ("CONSTELLATION_LOCK_TTL_MS", "15000"),
            (
                "RUST_LOG",
                "info,constellation_authority::core::locks=debug",
            ),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let holder = 0usize;
        let backups = wait_for_backup(&clients[holder], Duration::from_secs(30))?;
        let backup_idx = (0..clients.len())
            .find(|i| node_id(&clients[*i]).ok() == Some(backups[0]))
            .context("backup is not one of ours")?;
        let locker_idx = (1..clients.len())
            .find(|i| *i != backup_idx)
            .expect("four nodes");
        let other_idx = (1..clients.len())
            .find(|i| *i != backup_idx && *i != locker_idx)
            .expect("four nodes");
        let epoch = lease_of(&clients[holder])?["epoch"].as_u64().unwrap_or(0);
        eprintln!(
            "    {NAME}: {} holds epoch {epoch}; backup {}; locker {}; contender {}",
            clients[holder].name,
            clients[backup_idx].name,
            clients[locker_idx].name,
            clients[other_idx].name
        );
        let fb = clients[locker_idx].mnt.join("f");
        let held = hold_flock(fb.clone(), libc::LOCK_EX, Duration::from_secs(20))?;
        let mut wb = open_rw(&fb)?;
        write_at(&mut wb, 0, b"before")?;
        let fc = open_rw(&clients[other_idx].mnt.join("f"))?;
        anyhow::ensure!(
            flock(&fc, libc::LOCK_EX | libc::LOCK_NB) == Err(libc::EWOULDBLOCK),
            "the contender got the lock while the locker holds it"
        );
        clients[holder].kill9()?;
        let killed = Instant::now();
        let took = wait_holds(&clients[backup_idx], epoch, Duration::from_secs(20))?;
        eprintln!(
            "    {NAME}: {} took over after {took:?}",
            clients[backup_idx].name
        );
        // Through and after the failover: the locker writes without EIO,
        // the contender is refused.
        let mut eios = 0;
        let mut refused = 0;
        let until = killed + Duration::from_secs(12);
        while Instant::now() < until {
            match write_at(&mut wb, 0, b"during") {
                Ok(()) => {}
                Err(e) if e.raw_os_error() == Some(libc::EIO) => eios += 1,
                Err(e) => bail!("locker's write: {e}"),
            }
            match flock(&fc, libc::LOCK_EX | libc::LOCK_NB) {
                Err(e) if e == libc::EWOULDBLOCK => refused += 1,
                Ok(()) => {
                    bail!("the contender got the lock while the locker holds it (after failover)")
                }
                Err(e) => bail!("contender flock: errno {e}"),
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        eprintln!(
            "    {NAME}: locker EIOs {eios}, contender refusals {refused} across the failover"
        );
        anyhow::ensure!(
            eios == 0,
            "the locker saw {eios} EIO(s) across the failover"
        );
        held.release();
        let (r, took) = flock_timed(&fc, libc::LOCK_EX);
        r.map_err(|e| anyhow::anyhow!("contender after unlock: errno {e}"))?;
        eprintln!("    {NAME}: contender granted {took:?} after the locker's unlock");
        let _ = flock(&fc, libc::LOCK_UN);
        print_locks(NAME, &clients[backup_idx]);
        print_locks(NAME, &clients[locker_idx]);
        clients[holder].mount()?;
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        Ok(())
    })();
    if result.is_err() {
        keep_logs(NAME, &clients);
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// On failure, the nodes' logs survive the tempdir (`/tmp/harness-m14-logs`).
fn keep_logs(scenario: &str, clients: &[Client]) {
    let dir = Path::new("/tmp/harness-m14-logs");
    let _ = std::fs::create_dir_all(dir);
    for c in clients {
        let log = c.mnt.parent().map(|p| p.join("mount.log"));
        if let Some(log) = log {
            let _ = std::fs::copy(log, dir.join(format!("{scenario}-{}.log", c.name)));
        }
    }
    eprintln!("    {scenario}: node logs kept under {}", dir.display());
}

// ------------------------------------------------------------------ lock-latency

/// The measurements: on a LAN, the first lock on a file from a
/// non-sequencer (one round trip), cached re-locks (none), the holder's
/// own locks, and a contended handoff; and on a lone node, `--locks
/// cluster` against `--locks local` (the cost to single-node workloads).
pub fn lock_latency(_seed: u64) -> Result<()> {
    lock_latency_lan()?;
    lock_latency_lone()
}

fn lock_latency_lan() -> Result<()> {
    const NAME: &str = "lock-latency";
    const N: usize = 200;
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b"], &[], 0)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        for i in 0..20 {
            std::fs::write(a.mnt.join(format!("l{i}")), b"x")?;
        }
        eventually("files visible on B", Duration::from_secs(30), || {
            anyhow::ensure!(b.mnt.join("l19").exists(), "l19 missing on B");
            Ok(())
        })?;
        let mut first = Vec::new();
        for i in 0..20 {
            let f = open_rw(&b.mnt.join(format!("l{i}")))?;
            let (r, took) = flock_timed(&f, libc::LOCK_EX);
            r.map_err(|e| anyhow::anyhow!("errno {e}"))?;
            first.push(took);
            let _ = flock(&f, libc::LOCK_UN);
        }
        eprintln!(
            "    {NAME}: B (non-sequencer) first LOCK_EX on a file: {}",
            dist(first)
        );
        let f = open_rw(&b.mnt.join("l0"))?;
        let mut relock = Vec::new();
        for _ in 0..N {
            let (r, took) = flock_timed(&f, libc::LOCK_EX);
            r.map_err(|e| anyhow::anyhow!("errno {e}"))?;
            relock.push(took);
            let _ = flock(&f, libc::LOCK_UN);
        }
        eprintln!(
            "    {NAME}: B cached re-lock (LOCK_EX after LOCK_UN, same file): {}",
            dist(relock)
        );
        let mut fc = Vec::new();
        for _ in 0..N {
            let t = Instant::now();
            fcntl_lock(&f, libc::F_SETLK, libc::F_WRLCK as i16, 0, 0)
                .map_err(|e| anyhow::anyhow!("errno {e}"))?;
            fcntl_lock(&f, libc::F_SETLK, libc::F_UNLCK as i16, 0, 0).ok();
            fc.push(t.elapsed());
        }
        eprintln!("    {NAME}: B cached fcntl F_SETLK+F_UNLCK: {}", dist(fc));
        let fa = open_rw(&a.mnt.join("l1"))?;
        let mut holder = Vec::new();
        for _ in 0..N {
            let (r, took) = flock_timed(&fa, libc::LOCK_EX);
            r.map_err(|e| anyhow::anyhow!("errno {e}"))?;
            holder.push(took);
            let _ = flock(&fa, libc::LOCK_UN);
        }
        eprintln!("    {NAME}: A (the sequencer) LOCK_EX: {}", dist(holder));
        // Contended handoff: A holds, B waits, A unlocks.
        let mut handoff = Vec::new();
        for _ in 0..10 {
            let held = hold_flock(a.mnt.join("l2"), libc::LOCK_EX, Duration::from_secs(20))?;
            let waiter = std::thread::spawn({
                let p = b.mnt.join("l2");
                move || -> Result<Duration> {
                    let f = open_rw(&p)?;
                    let (r, took) = flock_timed(&f, libc::LOCK_EX);
                    r.map_err(|e| anyhow::anyhow!("errno {e}"))?;
                    let _ = flock(&f, libc::LOCK_UN);
                    Ok(took)
                }
            });
            std::thread::sleep(Duration::from_millis(300));
            let t = Instant::now();
            held.release();
            let w = waiter.join().expect("waiter")?;
            handoff.push(t.elapsed().min(w));
        }
        eprintln!(
            "    {NAME}: contended handoff (A unlock -> B granted): {}",
            dist(handoff)
        );
        for c in &clients {
            print_locks(NAME, c);
        }
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// A lone node: cluster mode against local mode.
fn lock_latency_lone() -> Result<()> {
    const NAME: &str = "lock-latency";
    let (env, root) = setup(NAME)?;
    for mode in ["local", "cluster"] {
        let backend = format!("s3://{BUCKET}/{NAME}-{mode}-{}", ts());
        let mut c = Client::new(
            root.path(),
            &format!("lone-{mode}"),
            &env.direct_endpoint,
            &backend,
        )?
        .with_env("CONSTELLATION_LOCKS", mode);
        c.fs_create()?;
        c.mount()?;
        let r = (|| -> Result<()> {
            let p = c.mnt.join("f");
            std::fs::write(&p, b"x")?;
            let f = open_rw(&p)?;
            let mut v = Vec::new();
            for _ in 0..1000 {
                let (r, took) = flock_timed(&f, libc::LOCK_EX);
                r.map_err(|e| anyhow::anyhow!("errno {e}"))?;
                v.push(took);
                let _ = flock(&f, libc::LOCK_UN);
            }
            let mut fc = Vec::new();
            for _ in 0..1000 {
                let t = Instant::now();
                fcntl_lock(&f, libc::F_SETLK, libc::F_WRLCK as i16, 0, 0)
                    .map_err(|e| anyhow::anyhow!("errno {e}"))?;
                fcntl_lock(&f, libc::F_SETLK, libc::F_UNLCK as i16, 0, 0).ok();
                fc.push(t.elapsed());
            }
            // Writes on a locked file: the fence check's cost.
            let mut w = Vec::new();
            fcntl_lock(&f, libc::F_SETLK, libc::F_WRLCK as i16, 0, 0)
                .map_err(|e| anyhow::anyhow!("errno {e}"))?;
            let mut wf = open_rw(&p)?;
            for i in 0..1000u64 {
                let t = Instant::now();
                write_at(&mut wf, i % 64, b"y")?;
                w.push(t.elapsed());
            }
            eprintln!(
                "    {NAME}: lone node --locks {mode}: flock LOCK_EX+UN {} | fcntl SETLK+UNLCK {} | write under lock {}",
                dist(v),
                dist(fc),
                dist(w)
            );
            print_locks(NAME, &c);
            Ok(())
        })();
        let _ = c.unmount();
        r?;
    }
    Ok(())
}
