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
use constellation_types::Code;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::process::CommandExt;
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

/// `flock(2)`; `Err(code)` on failure.
fn flock(f: &File, op: libc::c_int) -> std::result::Result<(), Code> {
    if unsafe { libc::flock(f.as_raw_fd(), op) } == 0 {
        Ok(())
    } else {
        Err(Code::from_io_error(&std::io::Error::last_os_error()))
    }
}

fn flock_timed(f: &File, op: libc::c_int) -> (std::result::Result<(), Code>, Duration) {
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
) -> std::result::Result<(), Code> {
    let mut l: libc::flock = unsafe { std::mem::zeroed() };
    l.l_type = typ;
    l.l_whence = libc::SEEK_SET as i16;
    l.l_start = start;
    l.l_len = len;
    if unsafe { libc::fcntl(f.as_raw_fd(), cmd, &l) } == 0 {
        Ok(())
    } else {
        Err(Code::from_io_error(&std::io::Error::last_os_error()))
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
        let _ = tx.send(r.map(|_| took).map_err(|e| anyhow::anyhow!("flock {e}")));
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
            r == Err(Code::Again),
            "B's LOCK_EX|LOCK_NB while A holds: {r:?} (took {took:?})"
        );
        eprintln!("    {NAME}: B's non-blocking LOCK_EX refused (EWOULDBLOCK) in {took:?}");
        let started = Instant::now();
        let waiter = std::thread::spawn({
            let fb = fb.clone();
            move || -> Result<Duration> {
                let f = open_rw(&fb)?;
                let (r, took) = flock_timed(&f, libc::LOCK_EX);
                r.map_err(|e| anyhow::anyhow!("{e}"))?;
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
        anyhow::ensure!(r == Err(Code::Again), "A's upgrade with B shared: {r:?}");
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
            .map_err(|e| anyhow::anyhow!("A F_SETLK: {e}"))?;
        let fb2 = open_rw(&fb)?;
        let r = fcntl_lock(&fb2, libc::F_SETLK, libc::F_WRLCK as i16, 50, 10);
        anyhow::ensure!(
            r == Err(Code::Again) || r == Err(Code::Access),
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
            .map_err(|e| anyhow::anyhow!("A F_UNLCK: {e}"))?;
        fcntl_lock(&fb2, libc::F_SETLKW, libc::F_WRLCK as i16, 0, 100)
            .map_err(|e| anyhow::anyhow!("B F_SETLKW: {e}"))?;
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
        flock(&fa, libc::LOCK_EX | libc::LOCK_NB).map_err(|e| anyhow::anyhow!("{e}"))?;
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
                flock(&f, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("{e}"))?;
                *granted_at.lock().unwrap() = Some(Instant::now());
                std::thread::sleep(Duration::from_millis(500));
                let _ = flock(&f, libc::LOCK_UN);
                Ok(())
            }
        });
        // B keeps writing under its lock: EIO once fenced.
        let mut first_eio = None;
        // Liveness, not latency: a starved host may take longer, the
        // hang bound decides "never".
        let deadline = Instant::now()
            + Duration::from_millis(LOCK_TTL_MS * 3 + 5_000).max(crate::verify::hang_bound());
        while Instant::now() < deadline {
            match write_at(&mut wb, 0, b"B2") {
                Ok(()) => {}
                Err(e) if Code::from_os_error(&e) == Some(Code::Io) => {
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
        // Exclusion and the protocol's lower bound: exact.
        anyhow::ensure!(fenced <= granted, "C was granted before B was fenced");
        anyhow::ensure!(
            granted - cut >= Duration::from_millis(LOCK_TTL_MS / 2),
            "C granted too early: {:?}",
            granted - cut
        );
        waiter.join().expect("waiter")?;
        // The upper bound is latency: ttl + margin, plus host slack.
        let late = (cut, granted - cut);
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
        crate::hostlag::latency_bound(
            &format!("{NAME}: C's grant after the cut"),
            late.0,
            late.1,
            Duration::from_millis(LOCK_TTL_MS + 1_000),
            Duration::from_millis(LOCK_TTL_MS + 1_000 + 4_000),
        )?;
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

// ------------------------------------------------------------------ lock-holder-killed-contention

/// One node's share of the counter workload: `n` times, `flock(LOCK_EX)`,
/// read the 8-byte counter, add one, write it back, `fsync`, unlock.
/// `hold`: at increment `at`, with the lock held and before reading,
/// signal and wait (the node is killed meanwhile). Returns the
/// increments made, when each completed, and the error that stopped it.
struct CounterRun {
    done: u64,
    times: Vec<Instant>,
    error: Option<String>,
}

type Hold = (
    u64,
    std::sync::mpsc::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

fn counter_worker(path: PathBuf, n: u64, hold: Option<Hold>) -> CounterRun {
    use std::os::unix::fs::FileExt;
    let mut run = CounterRun {
        done: 0,
        times: Vec::new(),
        error: None,
    };
    let f = match open_rw(&path) {
        Ok(f) => f,
        Err(e) => {
            run.error = Some(format!("{e:#}"));
            return run;
        }
    };
    for i in 0..n {
        if let Err(e) = flock(&f, libc::LOCK_EX) {
            run.error = Some(format!("flock: {e}"));
            return run;
        }
        if let Some((at, tx, rx)) = &hold {
            if i == *at {
                let _ = tx.send(());
                let _ = rx.recv_timeout(Duration::from_secs(120));
            }
        }
        let step = (|| -> std::io::Result<()> {
            let mut buf = [0u8; 8];
            let got = f.read_at(&mut buf, 0)?;
            let cur = if got == 8 { u64::from_be_bytes(buf) } else { 0 };
            f.write_at(&(cur + 1).to_be_bytes(), 0)?;
            f.sync_all()
        })();
        if let Err(e) = step {
            run.error = Some(format!("increment {i}: {e}"));
            return run;
        }
        run.done += 1;
        run.times.push(Instant::now());
        if let Err(e) = flock(&f, libc::LOCK_UN) {
            run.error = Some(format!("unlock: {e}"));
            return run;
        }
    }
    run
}

/// When the gap of length `gap` between two of `times` (sorted) began.
fn gaps_at(times: &[Instant], gap: Duration) -> Instant {
    times
        .windows(2)
        .find(|w| w[1] - w[0] == gap)
        .map(|w| w[0])
        .unwrap_or_else(Instant::now)
}

fn read_counter(path: &Path) -> Result<u64> {
    let b = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    anyhow::ensure!(b.len() == 8, "{}: {} bytes", path.display(), b.len());
    Ok(u64::from_be_bytes(b[..8].try_into().unwrap()))
}

/// Four nodes contend on one `flock`ed counter (read, add one, write,
/// `fsync`), as the EC2 lock run did; the holder of the lock (a
/// non-sequencer) is killed with the lock held. The survivors are
/// stalled only until its grant is outwaited (`ttl + margin` from its
/// last renewal at the owner) and then hand the lock among themselves
/// with no further outwait; the counter is exact. Then a node is killed
/// while its request is *parked* at the owner, first in line: the next
/// waiter is granted as soon as the holder unlocks, not `ttl + margin`
/// later (a grant pushed to the dead waiter would have to be outwaited
/// first).
pub fn lock_holder_killed_contention(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-holder-killed-contention";
    const LOCK_TTL_MS: u64 = 3_000;
    // The lease's expiry margin (min(1 s, lease ttl / 4)): the owner's
    // outwait is `ttl + margin` after the holder's last renewal.
    const MARGIN_MS: u64 = 1_000;
    const PER_NODE: u64 = 30;
    const VICTIM_AT: u64 = 8;
    // Host slack on top of the protocol's own bound (a loaded CI host:
    // the handoff's flush, a fsync through S3).
    const SLACK_MS: u64 = 3_000;
    let ttl = LOCK_TTL_MS.to_string();
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c", "d"],
        &[
            ("CONSTELLATION_LOCK_TTL_MS", &ttl),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"),
            (
                "RUST_LOG",
                "info,constellation_authority::core::locks=debug",
            ),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        std::fs::write(clients[0].mnt.join("counter"), [0u8; 8])?;
        for c in &clients[1..] {
            eventually(
                "counter visible everywhere",
                Duration::from_secs(30),
                || {
                    anyhow::ensure!(read_counter(&c.mnt.join("counter"))? == 0);
                    Ok(())
                },
            )?;
        }
        // ---- phase 1: the lock holder is killed mid-contention.
        let victim = 2usize;
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let mut hold = Some((VICTIM_AT, held_tx, go_rx));
        let mut workers = Vec::new();
        for (i, c) in clients.iter().enumerate() {
            let path = c.mnt.join("counter");
            let h = if i == victim { hold.take() } else { None };
            workers.push(std::thread::spawn(move || {
                counter_worker(path, PER_NODE, h)
            }));
        }
        held_rx
            .recv_timeout(Duration::from_secs(120))
            .context("the victim never reached its increment with the lock held")?;
        clients[victim].kill9()?;
        let killed = Instant::now();
        let _ = go_tx.send(());
        eprintln!(
            "    {NAME}: killed {} holding the lock after {VICTIM_AT} increments",
            clients[victim].name
        );
        let runs: Vec<CounterRun> = workers
            .into_iter()
            .map(|w| w.join().expect("counter worker"))
            .collect();
        let mut survivors_done = 0;
        let mut after: Vec<Instant> = Vec::new();
        for (i, r) in runs.iter().enumerate() {
            if i == victim {
                eprintln!(
                    "    {NAME}: victim {}: {} increments, stopped by: {:?}",
                    clients[i].name, r.done, r.error
                );
                anyhow::ensure!(r.done == VICTIM_AT, "victim made {} increments", r.done);
                continue;
            }
            anyhow::ensure!(
                r.error.is_none() && r.done == PER_NODE,
                "survivor {}: {} of {PER_NODE} increments, error {:?}",
                clients[i].name,
                r.done,
                r.error
            );
            survivors_done += r.done;
            after.extend(r.times.iter().copied().filter(|t| *t > killed));
        }
        after.sort();
        let first = *after.first().context("no increment after the kill")?;
        let stall = first - killed;
        let drained = *after.last().unwrap() - killed;
        let mut gaps: Vec<Duration> = after.windows(2).map(|w| w[1] - w[0]).collect();
        gaps.sort();
        let max_gap = gaps.last().copied().unwrap_or_default();
        eprintln!(
            "    {NAME}: survivors stalled {stall:?} after the kill (bound: ttl + margin = {} ms, + {SLACK_MS} ms host slack); \
             {} increments after it, drained in {drained:?}, largest gap between them {max_gap:?}",
            LOCK_TTL_MS + MARGIN_MS,
            after.len()
        );
        // The upper bounds are latency (judged against the host-lag probe
        // once the exact checks below held); the lower bound and the
        // counts are the protocol's and stay exact.
        let phase1_latency = (killed, stall, max_gap, gaps_at(&after, max_gap));
        // And not less than the grant's remaining life: the dead holder
        // renewed at most `ttl/2 + ttl/4` before the kill, so its grant
        // outlives the kill by at least `ttl/4 + margin` at the owner.
        anyhow::ensure!(
            stall >= Duration::from_millis(LOCK_TTL_MS / 4 + MARGIN_MS),
            "the survivors got the lock {stall:?} after the kill: before the dead holder's grant expired"
        );
        // No second outwait among the living (one would stall everyone
        // for most of `ttl + margin` again): `expired == 1` below is the
        // exact check; the largest gap is latency, with a hard cap a
        // stuck release (an F3-shaped stall shorter than the hang watch's
        // bound) cannot hide under.
        anyhow::ensure!(
            max_gap < Duration::from_millis(5 * LOCK_TTL_MS),
            "a {max_gap:?} gap between increments after the recovery (5 x TTL: a stuck release?)"
        );
        let expected = survivors_done + VICTIM_AT;
        let got = read_counter(&clients[0].mnt.join("counter"))?;
        anyhow::ensure!(
            got == expected,
            "counter {got}, expected {expected} (lost or doubled increments)"
        );
        let expired = locks_of(&clients[0])?["recalls_expired"]
            .as_u64()
            .unwrap_or(0);
        eprintln!("    {NAME}: counter {got} exact; the owner outwaited {expired} grant(s)");
        anyhow::ensure!(
            expired == 1,
            "{expired} grants outwaited; expected the dead holder's only"
        );

        // ---- phase 2: a node killed while parked, first in line.
        clients[victim].mount()?;
        {
            let refs: Vec<&Client> = clients.iter().collect();
            wait_for_p2p(&refs)?;
        }
        let held = hold_flock(
            clients[1].mnt.join("counter"),
            libc::LOCK_EX,
            Duration::from_secs(30),
        )?;
        let spawn_waiter = |p: PathBuf| {
            std::thread::spawn(move || -> Result<Instant> {
                let f = open_rw(&p)?;
                flock(&f, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("{e}"))?;
                let at = Instant::now();
                let _ = flock(&f, libc::LOCK_UN);
                Ok(at)
            })
        };
        let dead = spawn_waiter(clients[victim].mnt.join("counter"));
        std::thread::sleep(Duration::from_millis(700));
        let next = spawn_waiter(clients[3].mnt.join("counter"));
        std::thread::sleep(Duration::from_millis(300));
        clients[victim].kill9()?;
        // Longer than the dead waiter's window (`ttl - margin` from its
        // last message): whatever it was sent now would lapse on arrival.
        std::thread::sleep(Duration::from_millis(LOCK_TTL_MS));
        let released = Instant::now();
        held.release();
        let granted = next.join().expect("waiter")?;
        let waited = granted.saturating_duration_since(released);
        let _ = dead.join();
        let expired2 = locks_of(&clients[0])?["recalls_expired"]
            .as_u64()
            .unwrap_or(0);
        eprintln!(
            "    {NAME}: with {} dead in the queue ahead of it, {} was granted {waited:?} after the unlock; outwaited grants {expired} -> {expired2}",
            clients[victim].name, clients[3].name
        );
        // An outwait of the dead waiter's grant is counted: exact. How
        // long the next waiter waited is latency.
        anyhow::ensure!(expired2 == expired, "a grant was outwaited in phase 2");
        for c in &clients {
            print_locks(NAME, c);
        }
        let (killed, stall, max_gap, gap_at) = phase1_latency;
        crate::hostlag::latency_bound(
            &format!("{NAME}: the survivors' stall for the dead holder's grant"),
            killed,
            stall,
            Duration::from_millis(LOCK_TTL_MS + MARGIN_MS),
            Duration::from_millis(LOCK_TTL_MS + MARGIN_MS + SLACK_MS),
        )?;
        crate::hostlag::latency_bound(
            &format!("{NAME}: the largest gap between increments after the recovery"),
            gap_at,
            max_gap,
            Duration::ZERO,
            Duration::from_millis(LOCK_TTL_MS),
        )?;
        crate::hostlag::latency_bound(
            &format!("{NAME}: the next waiter's grant after the unlock, a dead waiter ahead"),
            released,
            waited,
            Duration::ZERO,
            Duration::from_millis(LOCK_TTL_MS + MARGIN_MS) / 2,
        )?;
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

// ------------------------------------------------------------------ lock-fence-at-close

/// A lock holder writes under its lock, then is cut from the owner past
/// its grant's lease; another node takes the lock and writes. When the
/// old holder closes, what it wrote under the lapsed grant must be
/// discarded — its close fails with `EIO` — and the file holds the new
/// holder's data everywhere. Twice: `fcntl`, closed with the lock still
/// held (the close drops the POSIX lock; the fence must be checked
/// before), and `flock`, unlocked first and then closed (the unlock
/// lifts the fence; the data stays tainted).
pub fn lock_fence_at_close(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-fence-at-close";
    const LOCK_TTL_MS: u64 = 3_000;
    const OLD: &[u8] = b"B-OLD-UNDER-LOCK";
    const NEW: &[u8] = b"C-NEW-UNDER-LOCK";
    let ttl = LOCK_TTL_MS.to_string();
    let (_env, root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[
            ("CONSTELLATION_LOCK_TTL_MS", &ttl),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"),
            (
                "RUST_LOG",
                "info,constellation_authority::core::locks=debug,constellation::locks=debug",
            ),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let b = &clients[1];
        let c = &clients[2];
        let a_id = node_id(a)?;
        let b_id = node_id(b)?;
        for (variant, file, posix) in [
            ("fcntl, closed while locked", "g1", true),
            ("flock, unlocked then closed", "g2", false),
        ] {
            std::fs::write(a.mnt.join(file), b"initial")?;
            for n in [b, c] {
                eventually(
                    "the file visible everywhere",
                    Duration::from_secs(30),
                    || {
                        anyhow::ensure!(std::fs::read(n.mnt.join(file))? == b"initial");
                        Ok(())
                    },
                )?;
            }
            let lock = |f: &File, blocking: bool| -> std::result::Result<(), Code> {
                if posix {
                    let cmd = if blocking {
                        libc::F_SETLKW
                    } else {
                        libc::F_SETLK
                    };
                    fcntl_lock(f, cmd, libc::F_WRLCK as i16, 0, 0)
                } else {
                    flock(f, libc::LOCK_EX)
                }
            };
            let mut wb = open_rw(&b.mnt.join(file))?;
            lock(&wb, true).map_err(|e| anyhow::anyhow!("B's lock: {e}"))?;
            write_at(&mut wb, 0, OLD).context("B's write under its lock")?;
            // Cut B from the owner (A), both directions: its renewals
            // fail and its grant lapses.
            std::fs::write(
                super::m9::c_deny_path(root.path(), &a.name),
                format!("{b_id}\n"),
            )?;
            std::fs::write(
                super::m9::c_deny_path(root.path(), &b.name),
                format!("{a_id}\n"),
            )?;
            let cut = Instant::now();
            // C takes the lock (after the owner outwaited B's grant) and
            // writes the new content through.
            let fc = open_rw(&c.mnt.join(file))?;
            lock(&fc, true).map_err(|e| anyhow::anyhow!("C's lock: {e}"))?;
            let granted = cut.elapsed();
            {
                use std::os::unix::fs::FileExt;
                fc.write_at(NEW, 0)?;
                fc.sync_all()?;
            }
            if posix {
                fcntl_lock(&fc, libc::F_SETLK, libc::F_UNLCK as i16, 0, 0).ok();
            } else {
                let _ = flock(&fc, libc::LOCK_UN);
            }
            drop(fc);
            // B's I/O under the lapsed grant is fenced.
            let fenced = write_at(&mut wb, 0, OLD);
            anyhow::ensure!(
                fenced.as_ref().err().and_then(Code::from_os_error) == Some(Code::Io),
                "{variant}: B's write after its grant lapsed: {fenced:?}"
            );
            // Heal; B's application closes (or unlocks, then closes).
            let _ = std::fs::remove_file(super::m9::c_deny_path(root.path(), &a.name));
            let _ = std::fs::remove_file(super::m9::c_deny_path(root.path(), &b.name));
            if !posix {
                flock(&wb, libc::LOCK_UN).map_err(|e| anyhow::anyhow!("B's unlock: {e}"))?;
            }
            let fd = std::os::unix::io::IntoRawFd::into_raw_fd(wb);
            let rc = unsafe { libc::close(fd) };
            let code = Code::from_os_error(&std::io::Error::last_os_error());
            eprintln!(
                "    {NAME}: {variant}: C granted {granted:?} after the cut; B's close returned {rc} ({code:?})"
            );
            anyhow::ensure!(
                rc == -1 && code == Some(Code::Io),
                "{variant}: B's close of data written under a lapsed grant returned {rc} ({code:?}), expected EIO"
            );
            for n in [a, b, c] {
                eventually(
                    "the new holder's data everywhere",
                    Duration::from_secs(30),
                    || {
                        let got = std::fs::read(n.mnt.join(file))?;
                        anyhow::ensure!(
                            got == NEW,
                            "{variant}: {} reads {:?}",
                            n.name,
                            String::from_utf8_lossy(&got)
                        );
                        Ok(())
                    },
                )?;
            }
            // Nothing stale is published later either (B's release, a
            // later close on B).
            std::fs::read(b.mnt.join(file))?;
            std::thread::sleep(Duration::from_millis(1_500));
            for n in [a, b, c] {
                let got = std::fs::read(n.mnt.join(file))?;
                anyhow::ensure!(
                    got == NEW,
                    "{variant}: {} reads {:?} later",
                    n.name,
                    String::from_utf8_lossy(&got)
                );
            }
            eprintln!("    {NAME}: {variant}: the file holds C's data on every node");
        }
        for c in &clients {
            print_locks(NAME, c);
        }
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
            flock(&fc, libc::LOCK_EX | libc::LOCK_NB) == Err(Code::Again),
            "the contender got the lock while the locker holds it"
        );
        let fenced0 = locks_of(&clients[locker_idx])?["fenced_io"]
            .as_u64()
            .unwrap_or(0);
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
                Err(e) if Code::from_os_error(&e) == Some(Code::Io) => eios += 1,
                Err(e) => bail!("locker's write: {e}"),
            }
            match flock(&fc, libc::LOCK_EX | libc::LOCK_NB) {
                Err(Code::Again) => refused += 1,
                Ok(()) => {
                    bail!("the contender got the lock while the locker holds it (after failover)")
                }
                Err(e) => bail!("contender flock: {e}"),
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        eprintln!(
            "    {NAME}: locker EIOs {eios}, contender refusals {refused} across the failover"
        );
        if eios > 0 {
            // An error to the application, not latency: a hard failure
            // whatever the host did. What triage needs: were they fences
            // (the grant lapsed: its renewal came `(ttl - margin) / 2`
            // late), and how much time the host took meanwhile.
            let fenced = locks_of(&clients[locker_idx])?["fenced_io"]
                .as_u64()
                .unwrap_or(0)
                .saturating_sub(fenced0);
            bail!(
                "the locker saw {eios} EIO(s) across the failover ({fenced} fenced by its lapsed \
                 grant; {})",
                crate::hostlag::lag_between(killed, until)
            );
        }
        held.release();
        let (r, took) = flock_timed(&fc, libc::LOCK_EX);
        r.map_err(|e| anyhow::anyhow!("contender after unlock: {e}"))?;
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

// ------------------------------------------------------------------ s3-cut-holder-failover-with-locks

/// An exclusive `flock` held by a child process (`flock(1)` around a
/// `sleep`): an application of its own, so that the owner fence of a
/// lapsed grant (EIO on every file of the mount for that owner, until its
/// locks are gone) falls on the child alone and not on the harness
/// process that measures reads and writes on the same mount.
pub(super) struct ChildLock(std::process::Child);

impl ChildLock {
    pub(super) fn take(path: &Path, other_mount: &Path) -> Result<Self> {
        let child = std::process::Command::new("flock")
            .arg("-x")
            .arg(path)
            .args(["sleep", "3600"])
            .stdin(std::process::Stdio::null())
            // Its own process group: `flock(1)` forks the command, so the
            // whole group is killed (an orphaned `sleep` would hold the
            // harness's output pipe open).
            .process_group(0)
            .spawn()
            .context("running flock(1)")?;
        let held = ChildLock(child);
        // Held once another node's non-blocking attempt is refused.
        let probe = open_rw(other_mount)?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match flock(&probe, libc::LOCK_EX | libc::LOCK_NB) {
                Err(Code::Again) => return Ok(held),
                Ok(()) => {
                    let _ = flock(&probe, libc::LOCK_UN);
                }
                Err(e) => bail!("probing the child's lock: {e}"),
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "the child never took its lock on {}",
                path.display()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for ChildLock {
    fn drop(&mut self) {
        // SAFETY: a plain signal to the group this child leads.
        unsafe {
            libc::kill(-(self.0.id() as libc::pid_t), libc::SIGKILL);
        }
        let _ = self.0.wait();
    }
}

/// Plan 43 C7-F F-a's tripwire on [`s3_cut_holder_failover_with_locks`]:
/// the cut holder beats until its lease ends, so its successor's floor is
/// capped at the lease's end plus one expiry margin (1 s: `EXPIRY_MARGIN_MS`
/// of the engine's lease) and the first write acknowledged and the first
/// lock granted after the takeover come within the margin and two seconds
/// of slack. (Without the cap the whole floor stands: +8.6 s on writes and
/// +23.6 s on locks past the takeover.)
const CAP_SLACK: Duration = Duration::from_secs(3);

/// What one application thread saw: `(started, returned, answer)` of each
/// op, relative to the S3 cut.
type Seen = Vec<(Duration, Duration, std::result::Result<(), Code>)>;

/// Plan 43 C7-H2's cost (gate G5), measured: the holder loses S3 but not
/// P2P, so it stays alive and keeps its backup's heartbeats while its
/// lease runs out; the backup seals once it reads the lease expired and
/// takes over. A tenure that served strict reads and locks makes that
/// successor wait the whole floor (the read-delegation TTL and the lock
/// TTL plus the takeover window and margins) before it acknowledges a
/// write or grants a lock. Four nodes, `--cto strict`: a reader keeps a
/// read delegation, and a locker (a child process: the lock owner's fence
/// must not fall on the reader) holds an exclusive lock on the same
/// mount; a writer creates files; a contender tries a non-blocking lock
/// on a free file every 200 ms. Printed, relative to the cut: the
/// takeover, the first write acknowledged and the first lock granted
/// after it, the longest write, every errno the applications saw, and how
/// often the nodes' logs refused a lock owner whose grant lapsed (the
/// owner fence: `EIO` for that owner on every file of the mount). Failed
/// by a write error, a read error (the reader is no lock owner) or a lock
/// attempt that failed at all: a non-blocking request on a free lock waits
/// for the successor's grace and is granted at its end (plan 43 F-b; it
/// answered `EAGAIN` for the whole floor before). Printed with the errnos:
/// the longest attempt, which is the longest wait.
/// Plan 43 C7-F F-a: the first write acknowledged and the first lock
/// granted after the takeover must come within [`CAP_SLACK`] of it (the
/// successor's floor ends at the cut holder's last beat's lease plus a
/// margin), and the status counters must show the cap at work
/// (`ack_floor_capped` or `ack_floor_capped_out`).
/// `CONSTELLATION_HARNESS_LOCKER_IN_PROCESS=1` holds the lock in the
/// harness process instead, the control: the reader then sees the fence.
pub fn s3_cut_holder_failover_with_locks(_seed: u64) -> Result<()> {
    const NAME: &str = "s3-cut-holder-failover-with-locks";
    // `CONSTELLATION_HARNESS_LEASE_TTL_MS`: the production lease (60000)
    // instead of the harness cluster's 20 s: the holder is cut from S3 a
    // full lease before its backup may seal it, so the requests the stale
    // holder answers before the takeover show.
    let lease_ttl = std::env::var("CONSTELLATION_HARNESS_LEASE_TTL_MS").ok();
    // `CONSTELLATION_HARNESS_LOCK_DEBUG=1`: the owner's lock answers too
    // (every renewal: a lot of lines), for the held-answer lines.
    let rust_log = if std::env::var_os("CONSTELLATION_HARNESS_LOCK_DEBUG").is_some() {
        "info,constellation_authority::core::backup=info,constellation::locks=debug,\
         constellation_authority::core::locks=debug"
    } else {
        "info,constellation_authority::core::backup=info,constellation::locks=debug"
    };
    let mut extra = vec![("CONSTELLATION_CTO", "strict"), ("RUST_LOG", rust_log)];
    if let Some(ttl) = lease_ttl.as_deref() {
        extra.push(("CONSTELLATION_LEASE_TTL_MS", ttl));
    }
    let (_env, _root, mut clients, proxies) = cluster(NAME, &["a", "b", "c", "d"], &extra, 1)?;
    let result = (|| -> Result<()> {
        let holder = 0usize;
        let backups = wait_for_backup(&clients[holder], Duration::from_secs(30))?;
        let backup_idx = (0..clients.len())
            .find(|i| node_id(&clients[*i]).ok() == Some(backups[0]))
            .context("backup is not one of ours")?;
        let reader_idx = (1..clients.len())
            .find(|i| *i != backup_idx)
            .expect("four nodes");
        let writer_idx = (1..clients.len())
            .find(|i| *i != backup_idx && *i != reader_idx)
            .expect("four nodes");
        let epoch = lease_of(&clients[holder])?["epoch"].as_u64().unwrap_or(0);
        eprintln!(
            "    {NAME}: {} holds epoch {epoch}; backup {}; reader+locker {}; writer+contender {}",
            clients[holder].name,
            clients[backup_idx].name,
            clients[reader_idx].name,
            clients[writer_idx].name
        );
        // The tenure serves strict reads and locks: a delegation on `f`,
        // an exclusive lock on `h` held through the run.
        for _ in 0..5 {
            std::fs::read(clients[reader_idx].mnt.join("f")).context("reading f")?;
        }
        let in_process = std::env::var_os("CONSTELLATION_HARNESS_LOCKER_IN_PROCESS").is_some();
        let (held, held_child) = if in_process {
            (
                Some(hold_flock(
                    clients[reader_idx].mnt.join("h"),
                    libc::LOCK_EX,
                    Duration::from_secs(20),
                )?),
                None,
            )
        } else {
            (
                None,
                Some(ChildLock::take(
                    &clients[reader_idx].mnt.join("h"),
                    &clients[writer_idx].mnt.join("h"),
                )?),
            )
        };
        let free = open_rw(&clients[writer_idx].mnt.join("g"))?;
        anyhow::ensure!(
            flock(&free, libc::LOCK_EX | libc::LOCK_NB).is_ok(),
            "the free lock was refused before the cut"
        );
        flock(&free, libc::LOCK_UN).ok();
        let stop = Arc::new(AtomicBool::new(false));
        let cut = Instant::now();
        let reads: Arc<std::sync::Mutex<Seen>> = Arc::default();
        let writes: Arc<std::sync::Mutex<Seen>> = Arc::default();
        let grabs: Arc<std::sync::Mutex<Seen>> = Arc::default();
        let spawn = |log: Arc<std::sync::Mutex<Seen>>,
                     mut op: Box<dyn FnMut(u64) -> std::result::Result<(), Code> + Send>,
                     every: u64| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut i = 0;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let started = cut.elapsed();
                    let r = op(i);
                    log.lock().unwrap().push((started, cut.elapsed(), r));
                    std::thread::sleep(Duration::from_millis(every));
                }
            })
        };
        let rpath = clients[reader_idx].mnt.join("f");
        let wdir = clients[writer_idx].mnt.clone();
        let gpath = clients[writer_idx].mnt.join("g");
        let t_reader = spawn(
            reads.clone(),
            Box::new(move |_| {
                std::fs::read(&rpath)
                    .map(|_| ())
                    .map_err(|e| Code::from_io_error(&e))
            }),
            100,
        );
        let t_writer = spawn(
            writes.clone(),
            Box::new(move |i| {
                std::fs::write(wdir.join(format!("w{i}")), b"w")
                    .map_err(|e| Code::from_io_error(&e))
            }),
            250,
        );
        let t_grab = spawn(
            grabs.clone(),
            Box::new(move |_| {
                let f = open_rw(&gpath).map_err(|_| Code::Io)?;
                let r = flock(&f, libc::LOCK_EX | libc::LOCK_NB);
                if r.is_ok() {
                    let _ = flock(&f, libc::LOCK_UN);
                }
                r
            }),
            200,
        );
        std::thread::sleep(Duration::from_secs(1));
        proxies[0].blackhole();
        let cut_at = cut.elapsed();
        eprintln!(
            "    {NAME}: {}'s S3 black-holed ({}); P2P stays up",
            clients[holder].name,
            lease_of(&clients[holder])?
        );
        let took = wait_holds(&clients[backup_idx], epoch, Duration::from_secs(150))?;
        let took_over = cut.elapsed();
        eprintln!(
            "    {NAME}: {} took over {:?} after the cut",
            clients[backup_idx].name,
            took_over - cut_at
        );
        let _ = took;
        // Until the first lock grant on the free file after the takeover
        // (and at least 20 s of writes past it).
        let grant_deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let granted = grabs
                .lock()
                .unwrap()
                .iter()
                .any(|(s, _, r)| *s >= took_over && r.is_ok());
            if granted || Instant::now() > grant_deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);
        for t in [t_reader, t_writer, t_grab] {
            t.join().ok();
        }
        let rel = |d: Duration| d.saturating_sub(cut_at);
        let first_ok_after = |log: &Seen, after: Duration| {
            log.iter()
                .filter(|(_, ret, r)| *ret >= after && r.is_ok())
                .map(|(_, ret, _)| rel(*ret))
                .min()
        };
        // Per errno: how many ops, and the first start and last return
        // (relative to the cut) of the ones that failed with it.
        let codes = |log: &Seen| {
            let mut m = std::collections::BTreeMap::<String, (usize, Duration, Duration)>::new();
            for (start, ret, r) in log {
                if let Err(c) = r {
                    let e = m
                        .entry(format!("{c:?}"))
                        .or_insert((0, rel(*start), rel(*ret)));
                    e.0 += 1;
                    e.2 = rel(*ret);
                }
            }
            m
        };
        let writes = writes.lock().unwrap();
        let grabs = grabs.lock().unwrap();
        let reads = reads.lock().unwrap();
        let longest = writes
            .iter()
            .max_by_key(|(s, r, _)| *r - *s)
            .map(|(s, r, _)| (rel(*s), *r - *s));
        eprintln!(
            "    {NAME}: takeover at +{:?}; first write acknowledged after it +{:?}; first lock \
             granted after it +{:?}; longest write {longest:?} ({} writes)",
            took_over - cut_at,
            first_ok_after(&writes, took_over),
            first_ok_after(&grabs, took_over),
            writes.len()
        );
        eprintln!(
            "    {NAME}: errnos as (count, first start, last return) after the cut: writes {:?} \
             ({} writes), non-blocking lock attempts {:?} ({} attempts), reads {:?} ({} reads)",
            codes(&writes),
            writes.len(),
            codes(&grabs),
            grabs.len(),
            codes(&reads),
            reads.len()
        );
        let fenced: usize = clients
            .iter()
            .map(|c| {
                c.log_text()
                    .matches("refused: the caller's lock grant lapsed")
                    .count()
            })
            .sum();
        eprintln!(
            "    {NAME}: lock owners refused for a lapsed grant in the nodes' logs: {fenced} \
             (locker {})",
            if in_process {
                "in the harness process"
            } else {
                "in a child process"
            }
        );
        let bad_write = !codes(&writes).is_empty();
        let bad_read = !in_process && !codes(&reads).is_empty();
        let bad_grab = !codes(&grabs).is_empty();
        let longest_grab = grabs
            .iter()
            .max_by_key(|(s, r, _)| *r - *s)
            .map(|(s, r, _)| (rel(*s), *r - *s));
        eprintln!(
            "    {NAME}: longest non-blocking lock attempt (start, took) {longest_grab:?}; \
             attempts that took over a second: {}",
            grabs
                .iter()
                .filter(|(s, r, _)| *r - *s > Duration::from_secs(1))
                .count()
        );
        let grace_waits: Vec<(String, u64, u64)> = clients
            .iter()
            .filter_map(|c| locks_of(c).ok().map(|l| (c.name.clone(), l)))
            .map(|(n, l)| {
                (
                    n,
                    l["grace_waits"].as_u64().unwrap_or(0),
                    l["grace_wait_gave_up"].as_u64().unwrap_or(0),
                )
            })
            .collect();
        eprintln!("    {NAME}: (node, grace_waits, grace_wait_gave_up) {grace_waits:?}");
        // What the waits and the answers were (plan 43 F-b): per node, the
        // requester's waits for an unreachable owner and refused waits,
        // the owner's answers to wait (stale owner, floor) and the answers
        // that went ahead of a held one, or waited for the order alone.
        let counters: Vec<_> = clients
            .iter()
            .filter_map(|c| locks_of(c).ok().map(|l| (c.name.clone(), l)))
            .map(|(n, l)| {
                let g = |k: &str| l[k].as_u64().unwrap_or(0);
                (
                    n,
                    [
                        ("unreachable_owner_waits", g("unreachable_owner_waits")),
                        ("refused_ring", g("grace_wait_refused_ring")),
                        ("refused_pool", g("grace_wait_refused_pool")),
                        ("stale_owner_answers", g("stale_owner_answers")),
                        ("not_yet_answers", g("not_yet_answers")),
                        ("order_bypassed", g("answers_order_bypassed")),
                        ("order_held", g("answers_order_held")),
                    ],
                )
            })
            .collect();
        eprintln!("    {NAME}: lock counters {counters:?}");
        let gave_up: u64 = grace_waits.iter().map(|(_, _, g)| *g).sum();
        let ack = super::m9::ack_of(&clients[backup_idx])?;
        let (capped, capped_out) = (
            super::m9::n(&ack, "ack_floor_capped"),
            super::m9::n(&ack, "ack_floor_capped_out"),
        );
        eprintln!(
            "    {NAME}: the successor's floor: armed {} time(s), shortened by the cap {capped}, \
             cap already past {capped_out} (saved {} ms)",
            super::m9::n(&ack, "ack_floor_waits"),
            super::m9::n(&ack, "ack_floor_cap_saved_ms")
        );
        let late =
            |first: Option<Duration>| first.is_none_or(|f| f > (took_over - cut_at) + CAP_SLACK);
        let (late_write, late_grab) = (
            late(first_ok_after(&writes, took_over)),
            late(first_ok_after(&grabs, took_over)),
        );
        proxies[0].heal();
        if let Some(held) = held {
            held.release();
        }
        drop(held_child);
        anyhow::ensure!(
            !bad_write && !bad_read && !bad_grab,
            "a write or a read failed, or a non-blocking lock attempt on a free file failed (see the lines above)"
        );
        anyhow::ensure!(
            first_ok_after(&grabs, took_over).is_some(),
            "no lock was granted on a free file within 90 s of the takeover"
        );
        anyhow::ensure!(
            gave_up == 0,
            "{gave_up} lock wait(s) gave up at the bound (the wait for the owner outlasted it)"
        );
        anyhow::ensure!(
            !late_write && !late_grab,
            "the successor's floor outlasted the cut holder's lease by more than {CAP_SLACK:?} \
             (first write acknowledged / first lock granted after the takeover: see the line \
             above): the beat cap did not apply"
        );
        anyhow::ensure!(
            capped + capped_out > 0,
            "the successor's floor was never capped by the cut holder's beats"
        );
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

// ------------------------------------------------------------------ lock-grace-wait

/// Three nodes after the holder was killed with a lock held (plan 43
/// F-b's setting): the backup has taken the lease over by seal and is in
/// the lock grace (`marker + takeover window + lock TTL + 2 × margin`,
/// about 18.5 s here), granting no new lock.
pub(super) struct GracedFailover {
    pub clients: Vec<Client>,
    /// The successor.
    pub backup: usize,
    /// The node that holds a lock (a child process of its own) and asks
    /// for the others in the grace.
    pub contender: usize,
    /// When the backup held the lease.
    pub took_over: Instant,
    locker: Option<ChildLock>,
    _env: crate::s3env::S3Env,
    _root: tempfile::TempDir,
}

impl GracedFailover {
    pub(super) fn keep_logs(&self, scenario: &str) {
        keep_logs(scenario, &self.clients);
    }

    /// The child's lock is let go (its process group killed) and every
    /// mount unmounted.
    pub(super) fn finish(&mut self) {
        drop(self.locker.take());
        // A probe still waiting on a lock (a scenario that failed before
        // joining it) holds its mount busy: unmounting waited for it for
        // ten minutes.
        kill_live_probes();
        for c in self.clients.iter_mut().rev() {
            let _ = c.unmount();
        }
    }
}

/// [`GracedFailover`]: `--cto strict`, a lock TTL of 15 s, `extra` env on
/// every mount; `files` are made on the contender's mount (the successor
/// acknowledges no write inside its floor); a child process on the
/// contender's node holds an exclusive lock (so the tenure is one a fast
/// successor waits out), the holder is killed and the backup takes over.
pub(super) fn graced_failover(
    scenario: &str,
    extra: &[(&str, &str)],
    files: &[&str],
) -> Result<GracedFailover> {
    let mut env_pairs = vec![
        ("CONSTELLATION_CTO", "strict"),
        ("CONSTELLATION_LOCK_TTL_MS", "15000"),
        (
            "RUST_LOG",
            "info,constellation_authority::core::locks=debug,constellation::locks=debug",
        ),
    ];
    env_pairs.extend_from_slice(extra);
    let (env, root, mut clients, _) = cluster(scenario, &["a", "b", "c"], &env_pairs, 0)?;
    let setup = (|| -> Result<(usize, usize, Instant, ChildLock)> {
        let backups = wait_for_backup(&clients[0], Duration::from_secs(30))?;
        let backup = (0..clients.len())
            .find(|i| node_id(&clients[*i]).ok() == Some(backups[0]))
            .context("backup is not one of ours")?;
        let contender = (1..clients.len())
            .find(|i| *i != backup)
            .expect("three nodes");
        let epoch = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        // The tenure serves strict reads and a lock.
        for _ in 0..5 {
            std::fs::read(clients[contender].mnt.join("f")).context("reading f")?;
        }
        for name in files {
            std::fs::write(clients[contender].mnt.join(name), b"x")
                .with_context(|| format!("making {name}"))?;
        }
        let locker = ChildLock::take(
            &clients[contender].mnt.join("h"),
            &clients[backup].mnt.join("h"),
        )?;
        eprintln!(
            "    {scenario}: {} holds epoch {epoch}; backup {}; contender {} (its child holds h)",
            clients[0].name, clients[backup].name, clients[contender].name
        );
        clients[0].kill9()?;
        wait_holds(&clients[backup], epoch, Duration::from_secs(30))?;
        Ok((backup, contender, Instant::now(), locker))
    })();
    match setup {
        Ok((backup, contender, took_over, locker)) => Ok(GracedFailover {
            clients,
            backup,
            contender,
            took_over,
            locker: Some(locker),
            _env: env,
            _root: root,
        }),
        Err(e) => {
            keep_logs(scenario, &clients);
            for c in clients.iter_mut().rev() {
                let _ = c.unmount();
            }
            Err(e)
        }
    }
}

/// Takes a probe off [`LIVE_PROBES`] when it has ended.
struct LiveProbe(u32);

impl Drop for LiveProbe {
    fn drop(&mut self) {
        LIVE_PROBES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|p| *p != self.0);
    }
}

/// What a `harness lock-probe nonblocking|waiter` child said and how it
/// ended: its stdout line, its exit code and how long it ran.
pub(super) struct Probed {
    pub said: String,
    pub code: i32,
    pub took: Duration,
}

/// The `lock-probe` children running now (their process ids): a scenario
/// that ends early kills them before it unmounts ([`kill_live_probes`]).
static LIVE_PROBES: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());

fn kill_live_probes() {
    let pids = std::mem::take(&mut *LIVE_PROBES.lock().unwrap_or_else(|e| e.into_inner()));
    for pid in &pids {
        // SAFETY: kill(2) of a child this process started and has not
        // reaped (it is removed from the list when it is).
        unsafe { libc::kill(*pid as i32, libc::SIGKILL) };
    }
    // The mount stays busy until they are gone (a process killed inside a
    // FUSE request dies when the daemon answers it, which a lock wait does
    // when its caller's signal reaches it): an unmount that raced them
    // failed `EBUSY` and its daemon then never exited, ten minutes.
    let end = Instant::now() + Duration::from_secs(30);
    while Instant::now() < end {
        let alive = pids
            .iter()
            .any(|pid| std::path::Path::new(&format!("/proc/{pid}")).exists());
        if !alive {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Run `lock-probe ROLE FILE INDEX` (this binary), pinned to `cpu` when
/// given, and wait for it (at most `within`).
pub(super) fn probe(
    role: &str,
    file: &Path,
    index: u64,
    cpu: Option<&str>,
    within: Duration,
) -> Result<std::thread::JoinHandle<Result<Probed>>> {
    use crate::spawn::TiedSpawn;
    let me = std::env::current_exe()?;
    let mut cmd = match cpu {
        Some(cpu) => {
            let mut c = std::process::Command::new("taskset");
            c.arg("-c").arg(cpu).arg(&me);
            c
        }
        None => std::process::Command::new(&me),
    };
    cmd.arg("lock-probe")
        .arg(role)
        .arg(file)
        .arg(index.to_string())
        .stdout(std::process::Stdio::piped());
    let started = Instant::now();
    let mut child = cmd.spawn_tied()?;
    let pid = child.id();
    LIVE_PROBES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(pid);
    let role = role.to_string();
    Ok(std::thread::spawn(move || {
        use std::io::Read;
        let mut _live = Some(LiveProbe(pid));
        let mut out = child.stdout.take().context("probe stdout")?;
        let reader = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = out.read_to_string(&mut s);
            s
        });
        let deadline = started + within;
        let status = loop {
            if let Some(st) = child.try_wait()? {
                // Reaped: its pid may be reused now, so it is off the list
                // of what `kill_live_probes` signals.
                drop(_live.take());
                break st;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                bail!("lock-probe {role} still running after {within:?}");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        Ok(Probed {
            said: reader.join().unwrap_or_default().trim().to_string(),
            code: status.code().unwrap_or(-1),
            took: started.elapsed(),
        })
    }))
}

/// Plan 43 F-b on a real kernel: the lock requests of an application
/// inside a failover's lock grace cost latency, not errors. Three nodes;
/// a child process on the contender's node holds a lock (the tenure serves
/// locks), the lock holder is killed, and the backup takes over: it grants
/// no new lock for the rest of its grace. In it the contender's node runs
/// three applications on free files: a non-blocking `F_SETLK` (waits, is
/// granted at the grace's end: on c9bee29 it answered `EAGAIN` at once), a
/// non-blocking one with a `SIGALRM` after eight seconds (ends `EINTR`,
/// as a blocking lock's wait would), and a blocking `F_SETLKW` (parked at the
/// successor, served by the timer at the grace's end). The lock the child
/// holds is still refused `EAGAIN` once the grace is over (a conflict is
/// no wait). Printed: how long each waited, and the lag of the blocking
/// waiter behind the non-blocking one (the grace-end timer serves it at
/// once; before it, at its next re-send). The nodes' counters: the
/// contender's `grace_waits` (2: the granted and the interrupted),
/// `grace_wait_gave_up` 0; the successor's `grace_wait_answers`.
pub fn lock_grace_wait(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-grace-wait";
    let mut g = graced_failover(NAME, &[], &["free1", "free2", "free3"])?;
    let result = (|| -> Result<()> {
        let mnt = g.clients[g.contender].mnt.clone();
        let within = Duration::from_secs(90);
        let nb = probe("nonblocking", &mnt.join("free1"), 0, None, within)?;
        // The signal comes 8 s into the request: after its first attempt
        // (a few seconds to find the new owner; that one answers when the
        // core does, as a blocking lock's does), well inside the grace.
        let intr = probe("nonblocking", &mnt.join("free2"), 8_000, None, within)?;
        let blocked = probe("waiter", &mnt.join("free3"), 0, None, within)?;
        let intr = intr.join().expect("probe thread")?;
        let nb = nb.join().expect("probe thread")?;
        let blocked = blocked.join().expect("probe thread")?;
        let since = |p: &Probed| p.took;
        eprintln!(
            "    {NAME}: {:?} after the takeover: non-blocking {:?} ({}), with a SIGALRM at 8 s \
             {:?} ({}), blocking {:?} ({})",
            g.took_over.elapsed(),
            since(&nb),
            nb.said,
            since(&intr),
            intr.said,
            since(&blocked),
            blocked.said
        );
        anyhow::ensure!(
            nb.code == 0,
            "the non-blocking lock on a free file in the grace must be granted, not refused \
             ({}, exit {})",
            nb.said,
            nb.code
        );
        anyhow::ensure!(
            nb.took >= Duration::from_secs(3),
            "the non-blocking lock did not wait for the grace ({:?}): is the grace over already?",
            nb.took
        );
        anyhow::ensure!(
            intr.code == 4
                && intr.took >= Duration::from_secs(8)
                && intr.took + Duration::from_secs(2) < nb.took,
            "the signal must end the wait with EINTR 8 s into it, well before the grace ends: {} \
             (exit {}, after {:?}; the non-blocking request took {:?})",
            intr.said,
            intr.code,
            intr.took,
            nb.took
        );
        anyhow::ensure!(
            blocked.code == 0,
            "the blocking lock must be granted: {} (exit {})",
            blocked.said,
            blocked.code
        );
        let lag = blocked.took.abs_diff(nb.took);
        eprintln!(
            "    {NAME}: the blocking waiter was granted {lag:?} from the non-blocking one \
             (the grace-end timer serves it at the end)"
        );
        // A conflict is no wait: the child's lock, now that the grace is
        // over.
        let probe_h = open_rw(&g.clients[g.backup].mnt.join("h"))?;
        let t = Instant::now();
        let r = flock(&probe_h, libc::LOCK_EX | libc::LOCK_NB);
        anyhow::ensure!(
            r == Err(Code::Again) && t.elapsed() < Duration::from_secs(3),
            "the held lock must be refused EAGAIN at once after the grace: {r:?} after {:?}",
            t.elapsed()
        );
        let on = locks_of(&g.clients[g.contender])?;
        let owner = locks_of(&g.clients[g.backup])?;
        eprintln!(
            "    {NAME}: contender grace_waits {} (ms total {}, gave up {}); successor \
             grace_wait_answers {}, grace_end_serves {}, grace_refusals {}",
            on["grace_waits"],
            on["grace_wait_ms_total"],
            on["grace_wait_gave_up"],
            owner["grace_wait_answers"],
            owner["grace_end_serves"],
            owner["grace_refusals"]
        );
        anyhow::ensure!(
            on["grace_waits"].as_u64().unwrap_or(0) >= 2 && on["grace_wait_gave_up"] == 0,
            "the contender must have counted both waits and no give-up: {on}"
        );
        anyhow::ensure!(
            owner["grace_wait_answers"].as_u64().unwrap_or(0) >= 1
                && owner["grace_end_serves"].as_u64().unwrap_or(0) >= 1,
            "the successor must have answered the wait and served its parked waiter at the grace's end: {owner}"
        );
        Ok(())
    })();
    if result.is_err() {
        g.keep_logs(NAME);
    }
    g.finish();
    result
}

/// On failure, the nodes' logs survive the tempdir, under this run's own
/// [`super::m11::kept_logs_dir`]. A fixed `/tmp/harness-m14-logs` was
/// shared by every run and every user on the host: concurrent runs
/// overwrote each other's logs, and with `fs.protected_regular=1` a run
/// as one user could not overwrite a log another user had left there.
fn keep_logs(scenario: &str, clients: &[Client]) {
    let dir = super::m11::kept_logs_dir();
    let _ = std::fs::create_dir_all(dir);
    for c in clients {
        let log = c.mnt.parent().map(|p| p.join("mount.log"));
        if let Some(log) = log {
            let _ = std::fs::copy(log, dir.join(format!("{scenario}-{}.log", c.name)));
        }
    }
    eprintln!("    {scenario}: node logs kept under {}", dir.display());
}

// ------------------------------------------------------------------ lock-lapse-lone-node

/// One op of a [`lock_lapse_lone_node`] worker: the name it made (or
/// renamed to) and what the kernel answered.
type LoneOutcome = (String, std::result::Result<(), Code>);

/// A worker issuing `op(i)` for i = 1, 2, … until `stop`.
fn lone_worker(
    stop: Arc<AtomicBool>,
    mut op: impl FnMut(u64) -> (String, std::io::Result<()>) + Send + 'static,
) -> std::thread::JoinHandle<Vec<LoneOutcome>> {
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let mut i = 0;
        while !stop.load(Ordering::Relaxed) {
            i += 1;
            let (what, r) = op(i);
            out.push((what, r.map_err(|e| Code::from_io_error(&e))));
            std::thread::sleep(Duration::from_millis(20));
        }
        out
    })
}

/// The xcheckfs differential run of 2026-10-13 (cell c2, a lone node,
/// `--write-mode back`): the host stalled the daemon for 18 s (an fjall
/// journal write held the metadata store's writer lock, and the authority
/// core waited on it), longer than what was left of a lock grant's window,
/// so the grant lapsed on the node that sequences it — the core that
/// renews it was the one stalled. The lock owner was xcheckfs itself (one
/// process issuing every stressor's requests), so the owner fence refused
/// every create, link, symlink, rename and write it sent from then on
/// (`EIO`), and a rename whose source an earlier refused rename never made
/// answered `ENOENT`.
///
/// That is the fence working as documented (cluster-locks.md, "Leases,
/// renewal and fencing"); what this pins is what the application sees on
/// a lone node: one process holds an `fcntl` lock, writes under it and
/// `fsync`s (published under an honoured grant), while its threads
/// create, link, symlink and rename; it writes the file again right
/// before the daemon is frozen past the grant's window and thawed. Every
/// op answers `OK` or `EIO`, and the tree holds exactly what answered
/// `OK` — an op in flight across the stall either applied and answered
/// `OK` or applied nothing; none is half-applied or reported wrongly. The
/// write after the lapse and the close under the fence answer `EIO`, the
/// file holds exactly what was `fsync`ed (the dirty write under the
/// lapsed grant is discarded, never published), the fence lifts with the
/// lock; after a remount the tree, the locked file and every write a
/// later `fsync` acknowledged are there, and fsck finds nothing.
pub fn lock_lapse_lone_node(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-lapse-lone-node";
    const LOCK_TTL_MS: u64 = 3_000;
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut c = Client::new(root.path(), "lone", &env.direct_endpoint, &backend)?
        .with_write_mode("back")
        .with_env("CONSTELLATION_LOCKS", "cluster")
        .with_env("CONSTELLATION_LOCK_TTL_MS", &LOCK_TTL_MS.to_string())
        .with_env(
            "RUST_LOG",
            "info,constellation_authority::core::locks=debug,constellation::locks=debug",
        );
    c.fs_create()?;
    c.mount()?;
    // Frozen well past the grant's window (ttl − margin from its last
    // renewal), well inside the lease's TTL.
    let result = lock_lapse_lone_run(
        NAME,
        &c,
        LoneStall::Freeze(Duration::from_millis(3 * LOCK_TTL_MS)),
    )
    .and_then(|expected| {
        // Read back from the bucket and the store, not this mount's
        // page cache.
        c.unmount()?;
        c.mount()?;
        lock_lapse_lone_check(NAME, &c, &expected, "after a remount")?;
        let fsck = c.fsck(false)?;
        let report = String::from_utf8_lossy(&fsck.stdout).to_string();
        anyhow::ensure!(
            fsck.status.success(),
            "fsck: {report}{}",
            String::from_utf8_lossy(&fsck.stderr)
        );
        let report: serde_json::Value = serde_json::from_str(&report).context("fsck's report")?;
        let mut shown = report.to_string();
        shown.truncate(400);
        eprintln!("    {NAME}: fsck clean: {shown}");
        Ok(())
    });
    if result.is_err() {
        keep_logs(NAME, std::slice::from_ref(&c));
    }
    let _ = c.unmount();
    result
}

/// Load campaign §5.3: `lock-lapse-lone-node`'s workload with the
/// daemon stalled from inside instead of frozen: one ordinary metadata
/// write holds the store's writer lock for 5, 12 and 20 s
/// (`CONSTELLATION_FAULT_STORE_STALL_FILE`), so the core's step waits
/// behind a slow FUSE writer (finding 08). Whether the grant lapses
/// depends on whether the core had to write meanwhile (the lock lane of
/// plan 41 L1-L4 renews without it; `CONSTELLATION_LOCK_LANE=off` then
/// makes this its fail-before); what must hold either way: every op
/// answers OK or EIO, the tree holds exactly what answered OK, the locked
/// file what its close published (or what was fsynced, when the close
/// was fenced), and fsck finds nothing, also after a remount.
pub fn lock_stall_lone_node(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-stall-lone-node";
    const LOCK_TTL_MS: u64 = 3_000;
    let stalls: Vec<u64> = std::env::var("CONSTELLATION_HARNESS_LONE_STALLS_MS")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .filter(|v: &Vec<u64>| !v.is_empty())
        .unwrap_or_else(|| vec![5_000, 12_000, 20_000]);
    for ms in stalls {
        let (env, root) = setup(NAME)?;
        let backend = format!("s3://{BUCKET}/{NAME}-{ms}-{}", ts());
        let file = root.path().join("store-stall");
        let mut c = Client::new(root.path(), "lone", &env.direct_endpoint, &backend)?
            .with_write_mode("back")
            .with_env("CONSTELLATION_LOCKS", "cluster")
            .with_env("CONSTELLATION_LOCK_TTL_MS", &LOCK_TTL_MS.to_string())
            .with_env(
                "CONSTELLATION_FAULT_STORE_STALL_FILE",
                &file.display().to_string(),
            )
            .with_env(
                "RUST_LOG",
                "info,constellation_authority::core::locks=debug,constellation::locks=debug",
            );
        c.fs_create()?;
        c.mount()?;
        eprintln!("    {NAME}: a {ms} ms store stall");
        let result =
            lock_lapse_lone_run(NAME, &c, LoneStall::Store { ms, file }).and_then(|expected| {
                c.unmount()?;
                c.mount()?;
                lock_lapse_lone_check(NAME, &c, &expected, "after a remount")?;
                let fsck = c.fsck(false)?;
                anyhow::ensure!(
                    fsck.status.success(),
                    "fsck: {}{}",
                    String::from_utf8_lossy(&fsck.stdout),
                    String::from_utf8_lossy(&fsck.stderr)
                );
                Ok(())
            });
        if result.is_err() {
            keep_logs(NAME, std::slice::from_ref(&c));
        }
        let _ = c.unmount();
        result.with_context(|| format!("the {ms} ms store stall"))?;
    }
    Ok(())
}

/// Load campaign §5.3, finding F1 in the real daemon: a fencing token
/// checked before the op waits for the store's writer lock, and not again
/// at the write, lets a write admitted under a grant land after the grant
/// ended. A lone node; `A` (a thread with its own open file description)
/// holds `flock` on `refs/main.lock`, `B` waits for it. `A`'s
/// `rename(refs/tmp-a, refs/main)` passes the check, then its write holds
/// for 10 s (the store-stall hook, armed for `A`'s task only), during
/// which the daemon is frozen 6 s — past the 3 s grant's window. Then `A`
/// resumes: its rename must answer `EIO` (ADR-25: an operation admitted
/// before a lapse and delayed past it never lands), `B` takes the lock
/// once `A` lets go and renames its own ref, and `refs/main` holds `B`'s,
/// also after a remount; fsck finds nothing. Before F1's fix (a re-check
/// under the writer lock) `A`'s rename answers OK.
pub fn fence_token_at_write(_seed: u64) -> Result<()> {
    const NAME: &str = "fence-token-at-write";
    const LOCK_TTL_MS: u64 = 3_000;
    const STALL_MS: u64 = 10_000;
    const FREEZE: Duration = Duration::from_secs(6);
    // The hook's `caller=` is the Linux kernel's task id, which only
    // Linux's FUSE passes on; another host only type-checks this.
    if !cfg!(target_os = "linux") {
        bail!(
            "{NAME}: the store stall is aimed at a FUSE caller's kernel task id, which is \
             Linux's (this is {})",
            std::env::consts::OS
        );
    }
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let file = root.path().join("store-stall");
    let mut c = Client::new(root.path(), "lone", &env.direct_endpoint, &backend)?
        .with_env("CONSTELLATION_LOCKS", "cluster")
        .with_env("CONSTELLATION_LOCK_TTL_MS", &LOCK_TTL_MS.to_string())
        .with_env(
            "CONSTELLATION_FAULT_STORE_STALL_FILE",
            &file.display().to_string(),
        )
        .with_env(
            "RUST_LOG",
            &format!(
                "info,constellation::locks=debug,{}",
                crate::verify::LOG_DIRECTIVES
            ),
        );
    c.fs_create()?;
    c.mount()?;
    let result = (|| -> Result<()> {
        let refs = c.mnt.join("refs");
        std::fs::create_dir(&refs)?;
        std::fs::write(refs.join("main"), b"base")?;
        std::fs::write(refs.join("main.lock"), b"")?;
        let (tid_tx, tid_rx) = std::sync::mpsc::channel::<u32>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let a = {
            let refs = refs.clone();
            std::thread::spawn(move || -> Result<(std::io::Result<()>, Duration)> {
                let f = open_rw(&refs.join("main.lock"))?;
                flock(&f, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("A's flock: {e}"))?;
                let mut t = File::create(refs.join("tmp-a"))?;
                t.write_all(b"A")?;
                t.sync_all()?;
                drop(t);
                // The kernel's task id on Linux (`gettid(2)`), which is
                // what FUSE names a request's caller by.
                let tid = constellation_platform::native()
                    .process
                    .current_thread()
                    .tid as u32;
                let _ = tid_tx.send(tid);
                let _ = go_rx.recv();
                let t0 = Instant::now();
                let r = std::fs::rename(refs.join("tmp-a"), refs.join("main"));
                let took = t0.elapsed();
                let _ = flock(&f, libc::LOCK_UN);
                Ok((r, took))
            })
        };
        let tid = tid_rx
            .recv_timeout(Duration::from_secs(60))
            .context("A never took the lock")?;
        // B queues behind A's lock (its own open file description).
        let b = {
            let refs = refs.clone();
            std::thread::spawn(move || -> Result<std::io::Result<()>> {
                let f = open_rw(&refs.join("main.lock"))?;
                flock(&f, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("B's flock: {e}"))?;
                let r = (|| {
                    let mut t = File::create(refs.join("tmp-b"))?;
                    t.write_all(b"B")?;
                    t.sync_all()?;
                    drop(t);
                    std::fs::rename(refs.join("tmp-b"), refs.join("main"))
                })();
                let _ = flock(&f, libc::LOCK_UN);
                Ok(r)
            })
        };
        std::thread::sleep(Duration::from_millis(500));
        let tmp = file.with_extension("tmp");
        std::fs::write(&tmp, format!("{STALL_MS} once caller={tid}"))?;
        std::fs::rename(&tmp, &file)?;
        let _ = go_tx.send(());
        // A's rename passed its check and holds the writer lock: freeze
        // the daemon past the grant's window.
        let deadline = Instant::now() + Duration::from_secs(5);
        while file.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        anyhow::ensure!(
            !file.exists(),
            "A's rename never took the store stall (caller {tid})"
        );
        std::thread::sleep(Duration::from_millis(300));
        c.pause()?;
        std::thread::sleep(FREEZE);
        c.resume()?;
        let (ra, took) = a.join().map_err(|_| anyhow::anyhow!("A panicked"))??;
        let rb = b.join().map_err(|_| anyhow::anyhow!("B panicked"))??;
        eprintln!(
            "    {NAME}: A's rename after {took:?} (stall {STALL_MS} ms, freeze {FREEZE:?}): \
             {ra:?}; B's: {rb:?}; locks {}",
            locks_of(&c)?
        );
        anyhow::ensure!(
            took >= Duration::from_millis(STALL_MS) - Duration::from_millis(500),
            "A's rename took {took:?}: the stall did not hold it"
        );
        match &ra {
            Err(e) if Code::from_os_error(e) == Some(Code::Io) => {}
            Ok(()) => bail!(
                "F1: A's rename passed its fencing-token check, waited {took:?} for the writer \
                 lock past its grant's end, and answered OK: a write under an ended grant landed"
            ),
            Err(e) => bail!("A's rename answered {e}, expected EIO (fenced)"),
        }
        rb.context("B's rename under its own grant")?;
        anyhow::ensure!(
            std::fs::read(refs.join("main"))? == b"B",
            "refs/main holds {:?}, expected B's ref",
            String::from_utf8_lossy(&std::fs::read(refs.join("main"))?)
        );
        c.unmount()?;
        c.mount()?;
        let got = std::fs::read(c.mnt.join("refs/main"))?;
        anyhow::ensure!(
            got == b"B",
            "after a remount refs/main holds {:?}, expected B's ref",
            String::from_utf8_lossy(&got)
        );
        let fsck = c.fsck(false)?;
        anyhow::ensure!(
            fsck.status.success(),
            "fsck: {}{}",
            String::from_utf8_lossy(&fsck.stdout),
            String::from_utf8_lossy(&fsck.stderr)
        );
        Ok(())
    })();
    let _ = c.resume();
    if result.is_err() {
        keep_logs(NAME, std::slice::from_ref(&c));
    }
    let _ = c.unmount();
    result
}

/// What the tree must hold once the fence lifted: each directory's
/// entries, and the plain file's words a successful `fsync` covers.
struct LoneExpected {
    dirs: std::collections::BTreeMap<&'static str, std::collections::BTreeSet<String>>,
    synced: Vec<u64>,
    /// What the locked file holds: what was `fsync`ed under the honoured
    /// grant when its close answered `EIO` (the dirty writes discarded),
    /// else every write that answered `OK`, in order.
    locked: Vec<u8>,
}

/// How [`lock_lapse_lone_run`] stalls the daemon past the grant's window.
enum LoneStall {
    /// `SIGSTOP` for this long: the grant lapses, by construction.
    Freeze(Duration),
    /// One ordinary metadata write holds the store's writer lock this
    /// long (`CONSTELLATION_FAULT_STORE_STALL_FILE` at `file`): the core
    /// waits behind it whenever it writes, finding 08's shape. Whether
    /// the grant lapses depends on whether the core had to write
    /// meanwhile; the correctness checks hold either way.
    Store { ms: u64, file: PathBuf },
}

/// The locked file's content: what was `fsync`ed under the honoured grant.
const LONE_SYNCED: &[u8] = b"UNDER-LOCK";
/// Written right before the stall, and after it.
const DIRTY: &[u8] = b"DIRTY-AT-LAPSE";
const AFTER: &[u8] = b"AFTER-LAPSE";

fn lock_lapse_lone_check(name: &str, c: &Client, e: &LoneExpected, when: &str) -> Result<()> {
    use std::os::unix::fs::FileExt;
    let m = &c.mnt;
    for (dir, want) in &e.dirs {
        let mut have = std::collections::BTreeSet::new();
        for d in std::fs::read_dir(m.join(dir))? {
            have.insert(d?.file_name().to_string_lossy().into_owned());
        }
        anyhow::ensure!(
            &have == want,
            "{when}: {dir}/ holds {:?} that answered EIO and lacks {:?} that answered OK",
            have.difference(want).collect::<Vec<_>>(),
            want.difference(&have).collect::<Vec<_>>()
        );
    }
    let locked = std::fs::read(m.join("locked"))?;
    anyhow::ensure!(
        locked == e.locked,
        "{when}: the locked file reads {:?}, expected {:?} (what was fsynced under the grant when \
         the close was fenced: the writes under a lapsed grant are discarded; else every write \
         that answered OK)",
        String::from_utf8_lossy(&locked),
        String::from_utf8_lossy(&e.locked)
    );
    let plain = File::open(m.join("plain"))?;
    for i in &e.synced {
        let mut b = [0u8; 8];
        plain.read_exact_at(&mut b, i * 8)?;
        anyhow::ensure!(
            u64::from_le_bytes(b) == *i,
            "{when}: write {i} answered OK and a later fsync too, but the file holds {b:?} there"
        );
    }
    eprintln!(
        "    {name}: {when}: the tree holds exactly what answered OK, the locked file what was fsynced, the plain file {} acknowledged writes",
        e.synced.len()
    );
    Ok(())
}

fn lock_lapse_lone_run(name: &str, c: &Client, stall: LoneStall) -> Result<LoneExpected> {
    use std::os::unix::fs::FileExt;
    let m = c.mnt.clone();
    for d in ["ren", "lnk", "sym", "cre"] {
        std::fs::create_dir(m.join(d))?;
    }
    std::fs::write(m.join("ren/r0"), b"r")?;
    std::fs::write(m.join("target"), b"t")?;
    std::fs::write(m.join("locked"), b"initial")?;
    let plain = Arc::new(open_rw(&m.join("plain"))?);
    let mut lf = open_rw(&m.join("locked"))?;
    fcntl_lock(&lf, libc::F_SETLKW, libc::F_WRLCK as i16, 0, 0)
        .map_err(|e| anyhow::anyhow!("the lock: {e}"))?;
    write_at(&mut lf, 0, LONE_SYNCED).context("the write under the lock")?;
    lf.sync_all().context("the fsync under the lock")?;

    let stop = Arc::new(AtomicBool::new(false));
    let ren = {
        let dir = m.join("ren");
        let mut cur = 0u64;
        lone_worker(stop.clone(), move |i| {
            let r = std::fs::rename(dir.join(format!("r{cur}")), dir.join(format!("r{i}")));
            if r.is_ok() {
                cur = i;
            }
            (format!("r{i}"), r)
        })
    };
    let lnk = {
        let (dir, target) = (m.join("lnk"), m.join("target"));
        lone_worker(stop.clone(), move |i| {
            let n = format!("k{i}");
            (n.clone(), std::fs::hard_link(&target, dir.join(n)))
        })
    };
    let sym = {
        let dir = m.join("sym");
        lone_worker(stop.clone(), move |i| {
            let n = format!("s{i}");
            (
                n.clone(),
                std::os::unix::fs::symlink("../target", dir.join(n)),
            )
        })
    };
    let cre = {
        let dir = m.join("cre");
        lone_worker(stop.clone(), move |i| {
            let n = format!("c{i}");
            let r = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.join(&n))
                .map(drop);
            (n, r)
        })
    };
    let wr = {
        let plain = plain.clone();
        lone_worker(stop.clone(), move |i| {
            (format!("{i}"), plain.write_all_at(&i.to_le_bytes(), i * 8))
        })
    };

    std::thread::sleep(Duration::from_secs(1));
    {
        // Thawed whatever happens below: a frozen daemon would hang the
        // workers and the unmount.
        struct Thaw<'a>(&'a Client);
        impl Drop for Thaw<'_> {
            fn drop(&mut self) {
                let _ = self.0.resume();
            }
        }
        // Dirty when the grant lapses: discarded at the close, never
        // published (it reads longer than what was fsynced).
        write_at(&mut lf, 0, DIRTY).context("the write right before the freeze")?;
        match &stall {
            LoneStall::Freeze(frozen) => {
                c.pause()?;
                let _thaw = Thaw(c);
                std::thread::sleep(*frozen);
            }
            LoneStall::Store { ms, file } => {
                // Whole, by a rename (a prefix would stall for less).
                let tmp = file.with_extension("tmp");
                std::fs::write(&tmp, format!("{ms} once"))?;
                std::fs::rename(&tmp, file)?;
                std::thread::sleep(Duration::from_millis(*ms + 1_000));
                anyhow::ensure!(
                    !file.exists(),
                    "no metadata write took the {ms} ms store stall"
                );
            }
        }
    }
    // The workers run into the fence (the grant lapsed while frozen).
    std::thread::sleep(Duration::from_secs(2));
    let after = write_at(&mut lf, 0, AFTER);
    stop.store(true, Ordering::Relaxed);
    let mut outcomes = std::collections::BTreeMap::new();
    for (what, h) in [
        ("rename", ren),
        ("link", lnk),
        ("symlink", sym),
        ("create", cre),
        ("write", wr),
    ] {
        let v = h
            .join()
            .map_err(|_| anyhow::anyhow!("the {what} worker panicked"))?;
        outcomes.insert(what, v);
    }
    let fence = locks_of(c)?;
    let freeze = matches!(stall, LoneStall::Freeze(_));
    let after_eio = after.as_ref().err().and_then(Code::from_os_error) == Some(Code::Io);
    // A freeze past the window lapses the grant by construction; a store
    // stall may not (only OK or EIO are right then).
    anyhow::ensure!(
        after_eio || (!freeze && after.is_ok()),
        "the write after the {} answered {after:?}, expected EIO{}; locks: {fence}",
        if freeze { "freeze" } else { "store stall" },
        if freeze { "" } else { " or OK" }
    );
    // The close drops the POSIX lock; the fence is checked before.
    let fd = std::os::unix::io::IntoRawFd::into_raw_fd(lf);
    let rc = unsafe { libc::close(fd) };
    let close = (rc == -1).then(|| Code::from_os_error(&std::io::Error::last_os_error()));
    anyhow::ensure!(
        close == Some(Some(Code::Io)) || (!after_eio && close.is_none()),
        "the close {} returned {rc} ({close:?}), expected EIO{}; locks: {fence}",
        if after_eio {
            "under the fence"
        } else {
            "after the stall"
        },
        if after_eio { "" } else { " or OK" }
    );
    let locked = if close.is_some() {
        LONE_SYNCED.to_vec()
    } else {
        // Every write answered OK and the close published them.
        let mut v = DIRTY.to_vec();
        v[..AFTER.len()].copy_from_slice(AFTER);
        v
    };
    let summary: Vec<String> = outcomes
        .iter()
        .map(|(what, v)| {
            let ok = v.iter().filter(|(_, r)| r.is_ok()).count();
            let eio = v.iter().filter(|(_, r)| *r == Err(Code::Io)).count();
            format!("{what} {ok} OK / {eio} EIO / {} other", v.len() - ok - eio)
        })
        .collect();
    eprintln!("    {name}: {}; locks {fence}", summary.join(", "));
    let fenced_ops = fence["owner_fenced_ops"].as_u64().unwrap_or(0);
    if let LoneStall::Freeze(frozen) = &stall {
        anyhow::ensure!(
            fenced_ops > 0,
            "the daemon was frozen {frozen:?} past a {}-ms lock window and fenced nothing: {fence}",
            fence["margin_ms"]
        );
    } else {
        eprintln!(
            "    {name}: the grant {} during the store stall ({fenced_ops} fenced ops)",
            if after_eio { "lapsed" } else { "held" }
        );
    }
    for (what, v) in &outcomes {
        if let Some((n, r)) = v.iter().find(|(_, r)| !matches!(r, Ok(()) | Err(Code::Io))) {
            bail!("{what} {n} answered {r:?}: only OK or EIO are right (the op was valid)");
        }
    }
    // The fence lifted with the lock: this process mutates again.
    std::fs::write(m.join("after-fence"), b"x").context("a create once the lock is gone")?;
    // The tree holds exactly what answered OK.
    let mut dirs = std::collections::BTreeMap::new();
    for (what, dir) in [("link", "lnk"), ("symlink", "sym"), ("create", "cre")] {
        let ok: std::collections::BTreeSet<String> = outcomes[what]
            .iter()
            .filter(|(_, r)| r.is_ok())
            .map(|(n, _)| n.clone())
            .collect();
        dirs.insert(dir, ok);
    }
    let renamed = outcomes["rename"]
        .iter()
        .rev()
        .find(|(_, r)| r.is_ok())
        .map(|(n, _)| n.clone())
        .unwrap_or_else(|| "r0".into());
    dirs.insert("ren", std::iter::once(renamed).collect());
    // What a successful fsync covers must be there: every write that
    // answered OK (none lost silently).
    let synced = plain.sync_all();
    eprintln!("    {name}: fsync of the plain file after the fence lifted: {synced:?}");
    let expected = LoneExpected {
        dirs,
        synced: if synced.is_ok() {
            outcomes["write"]
                .iter()
                .filter(|(_, r)| r.is_ok())
                .map(|(n, _)| n.parse())
                .collect::<std::result::Result<_, _>>()?
        } else {
            Vec::new()
        },
        locked,
    };
    drop(plain);
    lock_lapse_lone_check(name, c, &expected, "on the mount")?;
    let after = locks_of(c)?;
    anyhow::ensure!(
        after["fenced_owners"]
            .as_array()
            .is_some_and(|v| v.is_empty()),
        "a lock owner is still fenced: {after}"
    );
    Ok(expected)
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
            r.map_err(|e| anyhow::anyhow!("{e}"))?;
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
            r.map_err(|e| anyhow::anyhow!("{e}"))?;
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
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            fcntl_lock(&f, libc::F_SETLK, libc::F_UNLCK as i16, 0, 0).ok();
            fc.push(t.elapsed());
        }
        eprintln!("    {NAME}: B cached fcntl F_SETLK+F_UNLCK: {}", dist(fc));
        let fa = open_rw(&a.mnt.join("l1"))?;
        let mut holder = Vec::new();
        for _ in 0..N {
            let (r, took) = flock_timed(&fa, libc::LOCK_EX);
            r.map_err(|e| anyhow::anyhow!("{e}"))?;
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
                    r.map_err(|e| anyhow::anyhow!("{e}"))?;
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
                r.map_err(|e| anyhow::anyhow!("{e}"))?;
                v.push(took);
                let _ = flock(&f, libc::LOCK_UN);
            }
            let mut fc = Vec::new();
            for _ in 0..1000 {
                let t = Instant::now();
                fcntl_lock(&f, libc::F_SETLK, libc::F_WRLCK as i16, 0, 0)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                fcntl_lock(&f, libc::F_SETLK, libc::F_UNLCK as i16, 0, 0).ok();
                fc.push(t.elapsed());
            }
            // Writes on a locked file: the fence check's cost.
            let mut w = Vec::new();
            fcntl_lock(&f, libc::F_SETLK, libc::F_WRLCK as i16, 0, 0)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
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

// ------------------------------------------------------------------ lock-release-at-close

/// One leg of [`lock_release_at_close`]: the locking mode, the transport
/// asked for (`None`: the lane's), and the FUSE worker count (`None`: the
/// host's).
struct ReleaseLeg {
    locks: &'static str,
    transport: Option<&'static str>,
    threads: Option<&'static str>,
}

/// The last `close(2)` of an open file description releases its OFD and
/// `flock` locks before it returns, as on a local file system: another
/// description's non-blocking lock right after the close is granted, and
/// `F_OFD_GETLK` reports nothing. The kernel sends the `RELEASE` that
/// drops them in the background after the close's `FLUSH` was answered,
/// so a lock request served first saw them (`EAGAIN` in 40–80 % of tries,
/// xcheckfs on Linux 7.3); the view now waits for that `RELEASE` (the
/// engine's `view::lock_handles`). A `dup`'s close is not the last: the
/// lock stays.
///
/// A lone node, under `--locks cluster` over `/dev/fuse` with the host's
/// workers, over the transport ladder (`auto`: the ring where the kernel
/// offers it; the leg prints which it got), over `/dev/fuse` with a
/// single worker (no wait there: the kernel's queue order serves the
/// `RELEASE` first), and under `--locks local` (the kernel's own locks,
/// for the record). A single worker over the ring is not exercised: its
/// queue order holds only while the closing CPU's queue has a free
/// entry, which a test cannot arrange reliably.
///
/// The view waits for a `RELEASE` for `CONSTELLATION_LOCK_CLOSE_WAIT_MS`
/// (100 ms) at most, and a kernel can hold one back longer (a full
/// background queue, a loaded host: the documented limit). So a cluster
/// leg allows [`RELEASE_TOLERANCE`] of its rounds to see the closed lock:
/// 1 %, against 87 % (309 refusals and 124 reports in 500 rounds) before
/// the wait — a regression still fails by far, a CI host's stall does
/// not. Every round that saw it is printed with its delay.
pub fn lock_release_at_close(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-release-at-close";
    const ROUNDS: usize = 500;
    /// See the function doc.
    const RELEASE_TOLERANCE: usize = ROUNDS / 100;
    let legs = [
        ReleaseLeg {
            locks: "cluster",
            transport: Some("dev-fuse"),
            threads: None,
        },
        ReleaseLeg {
            locks: "cluster",
            transport: Some("auto"),
            threads: None,
        },
        ReleaseLeg {
            locks: "cluster",
            transport: Some("dev-fuse"),
            threads: Some("1"),
        },
        ReleaseLeg {
            locks: "local",
            transport: None,
            threads: None,
        },
    ];
    let (env, root) = setup(NAME)?;
    for (n, leg) in legs.iter().enumerate() {
        let mode = leg.locks;
        let backend = format!("s3://{BUCKET}/{NAME}-{n}-{mode}-{}", ts());
        let mut c = Client::new(
            root.path(),
            &format!("lone-{n}-{mode}"),
            &env.direct_endpoint,
            &backend,
        )?
        .with_env("CONSTELLATION_LOCKS", mode);
        if let Some(transport) = leg.transport {
            c = c.with_env("CONSTELLATION_FUSE_TRANSPORT", transport);
        }
        if let Some(threads) = leg.threads {
            c = c.with_env("CONSTELLATION_FUSE_THREADS", threads);
        }
        c.fs_create()?;
        c.mount()?;
        let r = (|| -> Result<()> {
            let got = c.control_status()?["mounts"][0]["transport"].clone();
            let label = format!(
                "--locks {mode}, transport {got} (asked {}), {} FUSE workers",
                leg.transport.unwrap_or("the lane's"),
                leg.threads.unwrap_or("the host's")
            );
            let p = c.mnt.join("f");
            std::fs::write(&p, b"x")?;
            let (mut refused, mut reported) = (0, 0);
            let mut took = Vec::new();
            for i in 0..ROUNDS {
                let flock_kind = i % 2 == 1;
                let a = open_rw(&p)?;
                let b = open_rw(&p)?;
                if flock_kind {
                    flock(&a, libc::LOCK_EX | libc::LOCK_NB)
                        .map_err(|e| anyhow::anyhow!("A's flock: {e}"))?;
                } else {
                    fcntl_lock(&a, libc::F_OFD_SETLK, libc::F_WRLCK as i16, 0, 10)
                        .map_err(|e| anyhow::anyhow!("A's F_OFD_SETLK: {e}"))?;
                }
                drop(a);
                let t = Instant::now();
                if !flock_kind && i % 4 == 0 {
                    // The test first: it must not report the closed lock.
                    let mut l: libc::flock = unsafe { std::mem::zeroed() };
                    l.l_type = libc::F_WRLCK as i16;
                    l.l_whence = libc::SEEK_SET as i16;
                    l.l_len = 10;
                    if unsafe { libc::fcntl(b.as_raw_fd(), libc::F_OFD_GETLK, &mut l) } != 0 {
                        bail!("F_OFD_GETLK: {}", std::io::Error::last_os_error());
                    }
                    if l.l_type != libc::F_UNLCK as i16 {
                        reported += 1;
                        eprintln!(
                            "    {NAME}: {label}: round {i}: F_OFD_GETLK reported the closed \
                             lock after {:?}",
                            t.elapsed()
                        );
                    }
                }
                let r = if flock_kind {
                    flock(&b, libc::LOCK_EX | libc::LOCK_NB)
                } else {
                    fcntl_lock(&b, libc::F_OFD_SETLK, libc::F_WRLCK as i16, 0, 10)
                };
                took.push(t.elapsed());
                if let Err(e) = r {
                    refused += 1;
                    eprintln!(
                        "    {NAME}: {label}: round {i} ({}): {e:?} after {:?}",
                        if flock_kind { "flock" } else { "OFD" },
                        t.elapsed()
                    );
                }
            }
            eprintln!(
                "    {NAME}: {label}: {ROUNDS} closes, then another description's lock: \
                 {refused} refused, {reported} F_OFD_GETLK reported the closed lock; took {}",
                dist(took)
            );
            print_locks(NAME, &c);
            // The kernel's own locks go synchronously: none, ever.
            let tolerance = if mode == "local" {
                0
            } else {
                RELEASE_TOLERANCE
            };
            anyhow::ensure!(
                refused + reported <= tolerance,
                "{label}: a lock of a closed description was still seen after close() \
                 returned ({refused} refusals, {reported} reports in {ROUNDS} rounds; \
                 {tolerance} allowed)"
            );
            // A dup's close is not the last: the lock stays.
            let a = open_rw(&p)?;
            let b = open_rw(&p)?;
            fcntl_lock(&a, libc::F_OFD_SETLK, libc::F_WRLCK as i16, 0, 10)
                .map_err(|e| anyhow::anyhow!("A's F_OFD_SETLK: {e}"))?;
            flock(&a, libc::LOCK_EX | libc::LOCK_NB)
                .map_err(|e| anyhow::anyhow!("A's flock: {e}"))?;
            drop(a.try_clone()?);
            let r = fcntl_lock(&b, libc::F_OFD_SETLK, libc::F_WRLCK as i16, 0, 10);
            anyhow::ensure!(
                r == Err(Code::Again),
                "{label}: OFD lock after a dup's close: {r:?}"
            );
            let r = flock(&b, libc::LOCK_EX | libc::LOCK_NB);
            anyhow::ensure!(
                r == Err(Code::Again),
                "{label}: flock after a dup's close: {r:?}"
            );
            drop(a);
            Ok(())
        })();
        let _ = c.unmount();
        r?;
    }
    Ok(())
}
