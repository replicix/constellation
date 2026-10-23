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

pub(super) fn flock_timed(f: &File, op: libc::c_int) -> (std::result::Result<(), Code>, Duration) {
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
/// (P2P denied both ways): its renewals fail and C — waiting for the lock
/// — is granted only after the owner outwaited B's grant (ttl + margin).
/// Plan 43 C7-F F-c: B's write under the lock does not fail when the grant
/// lapses, it **waits** (the owner cannot be reached to ask for the grant
/// back), and C is granted while it waits: B does no I/O under the lock
/// from the lapse on, which is the exclusion (no write of B is acknowledged
/// after C's grant). After the heal B's late renewal reaches the owner,
/// which granted the file to C meanwhile and so refuses it: the waiting
/// write answers `EIO` within seconds, and B locks again. (Before F-c the
/// write answered `EIO` at the lapse.)
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
        // B keeps writing under its lock, from a thread of its own: the
        // write that finds its grant lapsed waits, so the loop stops
        // there. Each write is logged with the time it was answered.
        type BWrite = (Instant, std::result::Result<(), Option<Code>>);
        let writes: Arc<std::sync::Mutex<Vec<BWrite>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = std::thread::spawn({
            let (writes, stop) = (writes.clone(), stop.clone());
            move || {
                while !stop.load(Ordering::Relaxed) {
                    let r = write_at(&mut wb, 0, b"B2").map_err(|e| Code::from_os_error(&e));
                    let failed = r.is_err();
                    writes.lock().unwrap().push((Instant::now(), r));
                    if failed {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                wb
            }
        });
        // Liveness, not latency: a starved host may take longer, the
        // hang bound decides "never".
        let deadline = Instant::now()
            + Duration::from_millis(LOCK_TTL_MS * 3 + 5_000).max(crate::verify::hang_bound());
        let granted = loop {
            if let Some(g) = *granted_at.lock().unwrap() {
                break g;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "C was never granted the lock within the deadline"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        // C holds the lock for 500 ms: B's write is still waiting through it.
        std::thread::sleep(Duration::from_millis(400));
        let answered = writes.lock().unwrap().len();
        std::thread::sleep(Duration::from_millis(100));
        {
            let w = writes.lock().unwrap();
            anyhow::ensure!(
                w.len() == answered,
                "B's write was answered while C held the lock: {:?}",
                w.last()
            );
            // The exclusion: none of B's writes was acknowledged after C's
            // grant, and none failed before it (it waits, it is not fenced).
            if let Some((t, r)) = w.iter().find(|(t, r)| r.is_err() && *t < granted) {
                bail!(
                    "B's write failed {:?} before C was granted ({r:?}): it should wait for the \
                     owner's verdict",
                    granted - *t
                );
            }
            anyhow::ensure!(
                w.iter().all(|(t, r)| r.is_err() || *t <= granted),
                "a write of B was acknowledged after C was granted the lock"
            );
        }
        eprintln!(
            "    {NAME}: C granted {:?} after the cut with B's write waiting ({answered} B writes \
             answered before)",
            granted - cut
        );
        anyhow::ensure!(
            granted - cut >= Duration::from_millis(LOCK_TTL_MS / 2),
            "C granted too early: {:?}",
            granted - cut
        );
        waiter.join().expect("waiter")?;
        // The upper bound is latency: ttl + margin, plus host slack.
        let late = (cut, granted - cut);
        // Heal: B's late renewal reaches the owner, which granted the file
        // to C since, and refuses it: the waiting write answers `EIO`.
        let _ = std::fs::remove_file(super::m9::c_deny_path(root.path(), &a.name));
        let _ = std::fs::remove_file(super::m9::c_deny_path(root.path(), &b.name));
        let healed = Instant::now();
        let fenced = loop {
            if let Some((t, r)) = writes.lock().unwrap().get(answered) {
                match r {
                    Err(Some(Code::Io)) => break *t,
                    other => bail!("B's waiting write answered {other:?}, expected EIO"),
                }
            }
            anyhow::ensure!(
                healed.elapsed() < Duration::from_secs(20),
                "B's waiting write was not answered within 20 s of the heal"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        eprintln!(
            "    {NAME}: B's write answered EIO {:?} after the heal ({:?} after the cut)",
            fenced - healed,
            fenced - cut
        );
        stop.store(true, Ordering::Relaxed);
        let mut wb = writer
            .join()
            .map_err(|_| anyhow::anyhow!("B's writer panicked"))?;
        // B's application unlocks (its grant is long gone), and can lock
        // again.
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
    /// How long each `flock(LOCK_EX)` took.
    waits: Vec<Duration>,
    error: Option<String>,
}

type Hold = (
    u64,
    std::sync::mpsc::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

/// `section`: how long each increment keeps the lock before it reads the
/// counter (the work an application does under it).
fn counter_worker(path: PathBuf, n: u64, hold: Option<Hold>, section: Duration) -> CounterRun {
    use std::os::unix::fs::FileExt;
    let mut run = CounterRun {
        done: 0,
        times: Vec::new(),
        waits: Vec::new(),
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
        let (locked, took) = flock_timed(&f, libc::LOCK_EX);
        if let Err(e) = locked {
            run.error = Some(format!("flock: {e}"));
            return run;
        }
        run.waits.push(took);
        if !section.is_zero() {
            std::thread::sleep(section);
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
                counter_worker(path, PER_NODE, h, Duration::ZERO)
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
/// granted once B unlocks — promptly, not after a released takeover's
/// grace (the successor's idle release can win the race with C's request).
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
        let unlocked = Instant::now();
        let (r, took) = flock_timed(&fc, libc::LOCK_EX);
        r.map_err(|e| anyhow::anyhow!("contender after unlock: {e}"))?;
        eprintln!("    {NAME}: contender granted {took:?} after the locker's unlock");
        let _ = flock(&fc, libc::LOCK_UN);
        print_locks(NAME, &clients[backup_idx]);
        print_locks(NAME, &clients[locker_idx]);
        // Nobody holds the lock once the locker unlocked: the contender is
        // granted at once (sub-millisecond), or after a lease handoff when
        // the successor's idle release won the race with its request. Not
        // after the grace of a released takeover (`ttl + margin`, 16 s
        // here): the successor's release left no grant behind, and its
        // lease says so. Half the lock TTL tells that grace from any
        // handoff a loaded host slows down.
        anyhow::ensure!(
            took < Duration::from_millis(7_500),
            "the contender waited {took:?} for a lock nobody held (a released takeover's \
             grace after a release that left no grant?); {}",
            crate::hostlag::lag_between(unlocked, Instant::now())
        );
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
/// harness process instead: the reader is then the lock owner itself, whose
/// grant lapses across the failover (the owner is cut from S3, its successor
/// exists only at the lease's end). Plan 43 C7-F F-c: its reads wait (park)
/// for the verdict on a late renewal of the same grant instead of failing
/// `EIO` (25 of 50 reads at the 20 s lease, 175 of 200 at the 60 s one,
/// before): the scenario fails on any read error here too, and prints the
/// parks the nodes counted and the longest read.
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
        let bad_read = !codes(&reads).is_empty();
        let longest_read = reads
            .iter()
            .max_by_key(|(s, r, _)| *r - *s)
            .map(|(s, r, _)| (rel(*s), *r - *s));
        eprintln!(
            "    {NAME}: reads: {} reads, {} failed; longest read (start, took) {longest_read:?}; \
             reads that took over a second: {}",
            reads.len(),
            reads.iter().filter(|(_, _, r)| r.is_err()).count(),
            reads
                .iter()
                .filter(|(s, r, _)| *r - *s > Duration::from_secs(1))
                .count()
        );
        // Plan 43 C7-F F-c: the nodes' parks, per node: grants that lapsed
        // under a lock or a write (`parks`), resumed by a late renewal,
        // refused, given up at the bound; ops that waited, ended by a signal
        // or refused for want of a ring entry or a thread; the longest park;
        // late renewals admitted and refused (the owners); final mirrors
        // sent, and installed by a successor.
        let parks: Vec<_> = clients
            .iter()
            .filter_map(|c| locks_of(c).ok().map(|l| (c.name.clone(), l)))
            .map(|(n, l)| {
                let g = |k: &str| l[k].as_u64().unwrap_or(0);
                (
                    n,
                    [
                        ("parks", g("lapse_parks")),
                        ("resumed", g("lapse_parks_resumed")),
                        ("refused", g("lapse_parks_refused")),
                        ("gave_up", g("lapse_park_gave_up")),
                        ("parked_ops", g("lapse_parked_ops")),
                        ("interrupted", g("lapse_park_interrupted")),
                        ("refused_ring", g("lapse_park_refused_ring")),
                        ("refused_pool", g("lapse_park_refused_pool")),
                        ("late_parks", g("lapse_late_parks")),
                        ("inline_waits", g("lapse_inline_waits")),
                        ("inline_ms", g("lapse_inline_wait_ms_total")),
                        ("inline_refused_thread", g("lapse_inline_refused_thread")),
                        ("ms_max", g("lapse_park_ms_max")),
                        ("late_admitted", g("late_admitted")),
                        ("late_refused", g("late_refused")),
                        ("final_sent", g("mirror_final_sent")),
                        ("final_installed", g("mirror_final_installed")),
                    ],
                )
            })
            .collect();
        eprintln!("    {NAME}: parks {parks:?}");
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
                        ("holder_reads", g("holder_reads")),
                        ("holder_reads_shared", g("holder_reads_shared")),
                        ("holder_reads_cached", g("holder_reads_cached")),
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

// ------------------------------------------------------------------ lock-contention-delegated

/// One node of a `lock-contention-delegated` round: waits `start_after`,
/// then `flock(LOCK_EX)` and one increment of the counter, `fsync`
/// included, then unlocks. Returns when the lock was granted and when it
/// was given up.
fn counter_turn(path: PathBuf, start_after: Duration) -> Result<(Instant, Instant, Duration)> {
    use std::os::unix::fs::FileExt;
    std::thread::sleep(start_after);
    let f = open_rw(&path)?;
    let (locked, took) = flock_timed(&f, libc::LOCK_EX);
    locked.map_err(|e| anyhow::anyhow!("flock: {e}"))?;
    let acquired = Instant::now();
    let mut buf = [0u8; 8];
    let got = f.read_at(&mut buf, 0)?;
    let cur = if got == 8 { u64::from_be_bytes(buf) } else { 0 };
    f.write_at(&(cur + 1).to_be_bytes(), 0)?;
    f.sync_all()?;
    let unlocked = Instant::now();
    flock(&f, libc::LOCK_UN).map_err(|e| anyhow::anyhow!("unlock: {e}"))?;
    Ok((acquired, unlocked, took))
}

/// Three nodes at the default lock TTL (20 s) and a delegation TTL of 5 s
/// (the core's default; the daemon's is the lock TTL: see `DELEG_TTL_MS`)
/// contend for one `flock`ed counter in a directory delegated to node b,
/// whose grants a 2 to 3 s delegation caps, while a waiter re-sends its
/// request every 9.5 s. Each round one node holds the lock for 4 s (an
/// application doing its work under it: a `git commit`), the other two ask
/// for it a third of a second after it, and every node increments once. A
/// grant pushed to a waiter whose request was seconds old reached it lapsed,
/// was recalled by the waiter behind it and outwaited: the waiters got the
/// lock seconds after the holder let go (sim `locks-delegated-production-ttl`:
/// 14 of 300 seeds never got a lock in a minute). Now the owner asks such a
/// waiter to send its request again and grants over it (plan 43 "ask
/// again"): the lock changes hands within `MEDIAN_GAP` of the unlock at the median
/// and `MAX_GAP` at most, the count is exact, and b asked (`asked_again`). Prints each handoff's gap, the
/// waits' distribution and the owner's `asked_again`, `ask_again_passed_over`,
/// `reserved_waits` and `recalls_expired`.
pub fn lock_contention_delegated(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-contention-delegated";
    // A short delegation, the core's own 5 s (`Config::delegation_ttl_ms`,
    // as the sim runs it): a delegate's grants are capped at 2 to 3 s.
    // The daemon's default is the lock TTL
    // (`Config::default_delegation_ttl_ms`), which caps them at ~17 s: no
    // stale waiter then (a push is old only past ~8 s). A deployment that
    // sets the delegation TTL short (`CONSTELLATION_DELEGATION_TTL_MS`) is
    // the one the rule is for; `CONSTELLATION_HARNESS_DELEGATION_TTL_MS=default`
    // runs the scenario at the daemon's default (nothing is asked then, and
    // the handoffs must be as fast).
    const DELEG_TTL_MS: u64 = 5_000;
    let at_default =
        std::env::var("CONSTELLATION_HARNESS_DELEGATION_TTL_MS").as_deref() == Ok("default");
    const ROUNDS: usize = 6;
    const HOLD: Duration = Duration::from_secs(4);
    // The handoff itself: the holder's unlock (a flush of what it wrote), the
    // owner's grant, the waiter's session wait, and its `fsync`. The bug's
    // signature is seconds: a re-send interval (9.5 s) or an outwait
    // (`ttl + margin` of a 2 to 3 s grant).
    const MAX_GAP: Duration = Duration::from_millis(2_500);
    const MEDIAN_GAP: Duration = Duration::from_millis(30);
    let (_env, _root, mut clients, _) = super::m11::cluster_with_deleg_ttl(
        NAME,
        &["a", "b", "c"],
        &[],
        0,
        (!at_default).then_some(DELEG_TTL_MS),
    )?;
    let result = (|| -> Result<()> {
        let b_id = node_id(&clients[1])?;
        std::fs::create_dir(clients[0].mnt.join("d1"))?;
        for c in &clients[1..] {
            eventually("d1 visible", Duration::from_secs(20), || {
                anyhow::ensure!(c.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        super::m11::delegate(&clients[0], "/d1", b_id)?;
        super::m11::wait_installed(&clients[1], "/d1", Duration::from_secs(20))?;
        std::fs::write(clients[0].mnt.join("d1/counter"), [0u8; 8])?;
        for c in &clients[1..] {
            eventually("counter visible", Duration::from_secs(30), || {
                anyhow::ensure!(read_counter(&c.mnt.join("d1/counter"))? == 0);
                Ok(())
            })?;
        }
        let mut gaps: Vec<Duration> = Vec::new();
        let mut waits: Vec<Duration> = Vec::new();
        let started = Instant::now();
        for round in 0..ROUNDS {
            let holder = round % 3;
            // The holder takes the lock, the others queue behind it.
            let hf = open_rw(&clients[holder].mnt.join("d1/counter"))?;
            flock(&hf, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("holder flock: {e}"))?;
            let waiters: Vec<_> = (0..3)
                .filter(|n| *n != holder)
                .map(|n| {
                    let path = clients[n].mnt.join("d1/counter");
                    std::thread::spawn(move || counter_turn(path, Duration::from_millis(300)))
                })
                .collect();
            std::thread::sleep(HOLD);
            {
                use std::os::unix::fs::FileExt;
                let mut buf = [0u8; 8];
                let got = hf.read_at(&mut buf, 0)?;
                let cur = if got == 8 { u64::from_be_bytes(buf) } else { 0 };
                hf.write_at(&(cur + 1).to_be_bytes(), 0)?;
                hf.sync_all()?;
            }
            let released = Instant::now();
            flock(&hf, libc::LOCK_UN).map_err(|e| anyhow::anyhow!("holder unlock: {e}"))?;
            drop(hf);
            let mut turns = Vec::new();
            for w in waiters {
                turns.push(w.join().expect("counter turn")?);
            }
            turns.sort_by_key(|t| t.0);
            let mut free_since = released;
            for (acquired, unlocked, took) in &turns {
                gaps.push(acquired.saturating_duration_since(free_since));
                waits.push(*took);
                free_since = *unlocked;
            }
            eprintln!(
                "    {NAME}: round {round} (held by {}): handoff gaps {:?}",
                clients[holder].name,
                &gaps[gaps.len() - turns.len()..]
            );
        }
        let longest = gaps.iter().max().copied().unwrap_or_default();
        eprintln!(
            "    {NAME}: handoff gaps (unlock to the next grant) {}; waits behind the 4 s hold {}",
            super::m8::dist(gaps.clone()),
            super::m8::dist(waits)
        );
        let expected = 3 * ROUNDS as u64;
        let got = read_counter(&clients[0].mnt.join("d1/counter"))?;
        let b = locks_of(&clients[1])?;
        let g = |k: &str| b[k].as_u64().unwrap_or(0);
        eprintln!(
            "    {NAME}: counter {got}; the delegate's asked_again {}, ask_again_passed_over {}, \
             reserved_waits {}, recalls_expired {}; asks answered by the requesters: a {} c {}",
            g("asked_again"),
            g("ask_again_passed_over"),
            g("reserved_waits"),
            g("recalls_expired"),
            locks_of(&clients[0])?["asks_answered"]
                .as_u64()
                .unwrap_or(0),
            locks_of(&clients[2])?["asks_answered"]
                .as_u64()
                .unwrap_or(0),
        );
        anyhow::ensure!(
            got == expected,
            "counter {got}, expected {expected} (lost or doubled increments)"
        );
        // The protocol's own cost of a handoff is the unlock's flush and the
        // grant's round trip (a few ms); a stale waiter's lapsed push cost
        // 50 ms more (its retry) and, now and then, an outwait. Judged
        // against the host-lag probe, as the other latency bounds are.
        let mut sorted = gaps.clone();
        sorted.sort();
        let median = sorted[sorted.len() / 2];
        crate::hostlag::latency_bound_over(
            &format!("{NAME}: the median handoff gap"),
            (started, Instant::now()),
            median,
            Duration::from_millis(5),
            MEDIAN_GAP,
        )?;
        // The bug's signature is seconds (a re-send interval, an outwait);
        // judged against the host-lag probe like the median.
        crate::hostlag::latency_bound_over(
            &format!("{NAME}: the longest handoff gap"),
            (started, Instant::now()),
            longest,
            Duration::from_millis(100),
            MAX_GAP,
        )?;
        // An outwaited grant in a run without a fault is a push that arrived
        // lapsed. The residual path is a transit slower than `margin / 4`
        // (a host stall): excused only by a stall the probe saw that long.
        let expired = g("recalls_expired");
        if expired > 0 {
            let lag = crate::hostlag::lag_between(started, Instant::now());
            anyhow::ensure!(
                lag.max >= Duration::from_millis(250),
                "the delegate outwaited {expired} grant(s) in a run without a fault, and no host \
                 stall of a quarter margin explains it (longest stall {:?})",
                lag.max
            );
            crate::hostlag::inconclusive(format!(
                "{NAME}: {expired} grant(s) outwaited; the host stalled {:?}",
                lag.max
            ));
        }
        anyhow::ensure!(
            at_default || g("asked_again") > 0,
            "the delegate never asked a stale waiter to ask again: the scenario did not exercise the rule"
        );
        Ok(())
    })();
    if result.is_err() {
        for c in &clients {
            print_locks(NAME, c);
        }
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

// ------------------------------------------------------------------ lock-contention-delegated-moves

/// `lock-contention-delegated` while a delegation move happens at every
/// handoff: 0.5 s after each unlock the root recalls the delegation of the
/// directory (`undelegate`) and delegates it to node b again (two moves of its
/// lock table, as a rename across the subtree's boundary makes). A ghost
/// contender queues first: a blocking `flock(1)` on the node that is neither
/// the holder nor the other waiter, killed as the holder lets go, so that a
/// grant served to it may reach its node with nothing waiting (the race with
/// its cancel: it does not always). A grant that nobody uses used to be
/// restamped at each move to a full lock TTL and margin (21 s at the default
/// TTL): the waiter behind it waited for that. Now it moves with what is left
/// of its record (`Core::carried_until`): the waiter is granted within a
/// delegate's capped grant (2 to 3 s), the margins of the moves and the slack
/// below, judged against the host-lag probe. The next hold starts once the
/// delegate serves again. Prints each gap and the nodes' `recalls_expired`
/// and `moved_carried`.
pub fn lock_contention_delegated_moves(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-contention-delegated-moves";
    const DELEG_TTL_MS: u64 = 5_000;
    const ROUNDS: usize = 6;
    const HOLD: Duration = Duration::from_secs(4);
    // A grant capped at 3 s and its margin, two moves' margins, and the
    // handoff itself; a restamp is 21 s per move.
    const MAX_GAP: Duration = Duration::from_millis(9_000);
    let (_env, _root, mut clients, _) =
        super::m11::cluster_with_deleg_ttl(NAME, &["a", "b", "c"], &[], 0, Some(DELEG_TTL_MS))?;
    let result = (|| -> Result<()> {
        let b_id = node_id(&clients[1])?;
        std::fs::create_dir(clients[0].mnt.join("d1"))?;
        for c in &clients[1..] {
            eventually("d1 visible", Duration::from_secs(20), || {
                anyhow::ensure!(c.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        super::m11::delegate(&clients[0], "/d1", b_id)?;
        super::m11::wait_installed(&clients[1], "/d1", Duration::from_secs(20))?;
        std::fs::write(clients[0].mnt.join("d1/counter"), [0u8; 8])?;
        for c in &clients[1..] {
            eventually("counter visible", Duration::from_secs(30), || {
                anyhow::ensure!(read_counter(&c.mnt.join("d1/counter"))? == 0);
                Ok(())
            })?;
        }
        // The mover: after each unlock, 0.5 s later, a rename across the
        // boundary (the recall) and a new delegation of d1 to b.
        let (go, moves) = std::sync::mpsc::channel::<()>();
        let (moved_tx, moved_rx) = std::sync::mpsc::channel::<()>();
        let root_client = &clients[0];
        let delegate_client = &clients[1];
        let window = Instant::now();
        let ran = std::thread::scope(|scope| -> Result<()> {
            let mover = scope.spawn(move || -> Result<usize> {
                let mut done = 0;
                while moves.recv().is_ok() {
                    std::thread::sleep(Duration::from_millis(500));
                    // The recall: the table moves to the root.
                    super::m11::undelegate(root_client, "/d1")?;
                    done += 1;
                    let end = Instant::now() + Duration::from_secs(10);
                    loop {
                        match super::m11::delegate(root_client, "/d1", b_id) {
                            Ok(_) => break,
                            Err(e) if Instant::now() < end => {
                                let _ = e;
                                std::thread::sleep(Duration::from_millis(100));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    super::m11::wait_installed(delegate_client, "/d1", Duration::from_secs(20))?;
                    let _ = moved_tx.send(());
                }
                Ok(done)
            });
            let mut gaps: Vec<Duration> = Vec::new();
            let started = Instant::now();
            for round in 0..ROUNDS {
                let holder = round % 3;
                // The ghost is on a node that is neither the holder nor the
                // owner (a local op's grant is installed at once and released
                // with the process).
                let ghost = if holder == 0 { 2 } else { 0 };
                let waiter = (0..3)
                    .find(|n| *n != holder && *n != ghost)
                    .expect("3 nodes");
                let hf = open_rw(&clients[holder].mnt.join("d1/counter"))?;
                flock(&hf, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("holder flock: {e}"))?;
                let ghost_child = std::process::Command::new("flock")
                    .arg("-x")
                    .arg(clients[ghost].mnt.join("d1/counter"))
                    .arg("true")
                    .stdin(std::process::Stdio::null())
                    .process_group(0)
                    .spawn()
                    .context("running flock(1) as the ghost")?;
                let ghost_pid = ghost_child.id() as libc::pid_t;
                let mut ghost_child = ghost_child;
                std::thread::sleep(Duration::from_millis(500));
                let path = clients[waiter].mnt.join("d1/counter");
                let live = std::thread::spawn(move || counter_turn(path, Duration::ZERO));
                std::thread::sleep(HOLD);
                {
                    use std::os::unix::fs::FileExt;
                    let mut buf = [0u8; 8];
                    let got = hf.read_at(&mut buf, 0)?;
                    let cur = if got == 8 { u64::from_be_bytes(buf) } else { 0 };
                    hf.write_at(&(cur + 1).to_be_bytes(), 0)?;
                    hf.sync_all()?;
                }
                // The ghost goes as the lock is let go.
                // SAFETY: a signal to the group this child leads.
                unsafe {
                    libc::kill(-ghost_pid, libc::SIGKILL);
                }
                let released = Instant::now();
                flock(&hf, libc::LOCK_UN).map_err(|e| anyhow::anyhow!("holder unlock: {e}"))?;
                drop(hf);
                let _ = go.send(());
                let (acquired, _unlocked, took) = live.join().expect("counter turn")?;
                let _ = ghost_child.wait();
                // The next hold starts once the move is over and the delegate
                // serves again: a holder whose grant straddles a move is a
                // different measure (its renewals meet the recall).
                match moved_rx.recv_timeout(Duration::from_secs(40)) {
                    Ok(()) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        // The mover ended: its error says why.
                        return match mover.join().expect("mover") {
                            Err(e) => Err(e.context("the mover failed")),
                            Ok(_) => Err(anyhow::anyhow!("the mover ended early")),
                        };
                    }
                    Err(e) => bail!("the move did not finish: {e}"),
                }
                let gap = acquired.saturating_duration_since(released);
                eprintln!(
                "    {NAME}: round {round} (held by {}, ghost {}, waiter {}): gap {gap:?}, waited {took:?}",
                clients[holder].name, clients[ghost].name, clients[waiter].name
            );
                gaps.push(gap);
            }
            drop(go);
            let moved = mover.join().expect("mover")?;
            let longest = gaps.iter().max().copied().unwrap_or_default();
            let expected = 2 * ROUNDS as u64;
            let got = read_counter(&clients[0].mnt.join("d1/counter"))?;
            for c in &clients {
                let l = locks_of(c)?;
                eprintln!(
                    "    {NAME}: {}: recalls_expired {}, moved_carried {}, grants_made {}",
                    c.name,
                    l["recalls_expired"].as_u64().unwrap_or(0),
                    l["moved_carried"].as_u64().unwrap_or(0),
                    l["grants_made"].as_u64().unwrap_or(0),
                );
            }
            eprintln!(
                "    {NAME}: counter {got}, {moved} moves; handoff gaps {}",
                super::m8::dist(gaps.clone())
            );
            anyhow::ensure!(
                got == expected,
                "counter {got}, expected {expected} (lost or doubled increments)"
            );
            anyhow::ensure!(moved >= ROUNDS, "{moved} moves in {ROUNDS} rounds");
            crate::hostlag::latency_bound_over(
                &format!("{NAME}: the longest handoff gap"),
                (started, Instant::now()),
                longest,
                Duration::from_millis(100),
                MAX_GAP,
            )?;
            Ok(())
        });
        // A holder whose grant lapsed because the host stalled (a core
        // step of seconds on a loaded box) is fenced: `EIO`. Not the rule's.
        match ran {
            Err(e)
                if e.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.raw_os_error() == Some(libc::EIO)) =>
            {
                let lag = crate::hostlag::lag_between(window, Instant::now());
                anyhow::ensure!(
                    lag.max >= Duration::from_millis(500),
                    "a holder was fenced (EIO) and no host stall explains it (longest {:?}): {e:#}",
                    lag.max
                );
                crate::hostlag::inconclusive(format!(
                    "{NAME}: a holder was fenced (EIO); the host stalled {:?}",
                    lag.max
                ));
                Ok(())
            }
            other => other,
        }
    })();
    if result.is_err() {
        for c in &clients {
            print_locks(NAME, c);
        }
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
        // The probe arms its 8 s alarm when it starts and its lock call
        // follows after the process start and the open of the file (2 s or
        // 4 s on the box, bimodal): it says how long the lock call waited
        // (`EINTR after`), the harness how long the whole probe took, and
        // the difference is the start. Printed, so that a slow start shows.
        let eintr_after = intr
            .said
            .strip_prefix("EINTR after ")
            .and_then(|d| d.strip_suffix('s'))
            .and_then(|d| d.parse::<f64>().ok())
            .map(Duration::from_secs_f64);
        eprintln!(
            "    {NAME}: the interrupted probe started {:?} before its lock call, the \
             non-blocking one {:?}",
            eintr_after.map(|d| intr.took.saturating_sub(d)),
            nb.said
                .strip_prefix("granted after ")
                .and_then(|d| d.strip_suffix('s'))
                .and_then(|d| d.parse::<f64>().ok())
                .map(|d| nb.took.saturating_sub(Duration::from_secs_f64(d)))
        );
        // The signal ends the wait inside the grace, not at its end: the
        // call's own wait is the 8 s of the alarm, and the probe's end
        // (its start, the alarm's 8 s) comes before the grace's end (the
        // non-blocking probe's) by a margin that a slow start and the
        // timer's wakeups cannot eat: half a second. (It was 2 s: with a
        // 4 s start the two ended 12.08 and 14.1 s after their spawns, 6 ms
        // from failing on the base daemon.)
        anyhow::ensure!(
            intr.code == 4
                && eintr_after.is_some_and(|d| d >= Duration::from_secs(8))
                && intr.took + Duration::from_millis(500) < nb.took,
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
/// That was the fence working as documented (cluster-locks.md, "Leases,
/// renewal and fencing"), and what this pins is what the application sees
/// on a lone node: one process holds an `fcntl` lock, writes under it and
/// `fsync`s (published under an honoured grant), while its threads
/// create, link, symlink and rename; it writes the file again right
/// before the daemon is frozen past the grant's window and thawed.
///
/// Plan 43 C7-F F-c: nothing could have been granted elsewhere (the node
/// sequences its own grants and still has the record), so after the thaw
/// the grant is asked for back from the node itself and admitted, and what
/// the freeze held up runs: after a freeze every op answers `OK` (none
/// `EIO`), the write after the lapse and the close answer `OK`, the file
/// holds the writes in order (the dirty one is published, not discarded),
/// the status counts the parks resumed and no op fenced. (Before F-c the
/// write and the close answered `EIO` and the file held what was
/// `fsync`ed.) A store stall (`lock-stall-lone-node`) may or may not lapse
/// the grant, and either way every op answers `OK` or `EIO` and the tree
/// holds exactly what answered `OK`; after a remount the tree, the locked
/// file and every write a later `fsync` acknowledged are there, and fsck
/// finds nothing.
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
/// daemon stalled from inside instead of frozen: one of the core's own
/// metadata commits holds the store's writer lock for 5, 12 and 20 s
/// (`CONSTELLATION_FAULT_STORE_STALL_FILE`, `core-only`), so the core's
/// step is stuck on a slow journal write (finding 08). With the lock lane of plan 41
/// (the default), which renews the node's own grant without the stuck core,
/// the grant does not lapse: no op answers `EIO`, no owner is fenced, the
/// lock is never lost, and the status counts the lane's own renewals.
/// `CONSTELLATION_HARNESS_LONE_LANE=off` runs the daemon with
/// `CONSTELLATION_LOCK_LANE=off` and expects the lapse instead (the
/// in-scenario fail-before: the fault point reaches the core). What must
/// hold in both: every op answers OK or EIO, the tree holds exactly what
/// answered OK, the locked file what its close published (or what was
/// fsynced, when the close was fenced), and fsck finds nothing, also after
/// a remount.
pub fn lock_stall_lone_node(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-stall-lone-node";
    const LOCK_TTL_MS: u64 = 3_000;
    let stalls: Vec<u64> = std::env::var("CONSTELLATION_HARNESS_LONE_STALLS_MS")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .filter(|v: &Vec<u64>| !v.is_empty())
        .unwrap_or_else(|| vec![5_000, 12_000, 20_000]);
    // `CONSTELLATION_HARNESS_LONE_LANE=off`: the daemon runs with
    // `CONSTELLATION_LOCK_LANE=off` and the scenario expects the lapse (the
    // in-scenario fail-before: the fault point is proven effective).
    let lane_off = std::env::var("CONSTELLATION_HARNESS_LONE_LANE").as_deref() == Ok("off");
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
                "CONSTELLATION_LOCK_LANE",
                if lane_off { "off" } else { "on" },
            )
            .with_env(
                "RUST_LOG",
                "info,constellation_authority::core::locks=debug,constellation::locks=debug",
            );
        c.fs_create()?;
        c.mount()?;
        eprintln!("    {NAME}: a {ms} ms store stall");
        let result = lock_lapse_lone_run(
            NAME,
            &c,
            LoneStall::Store {
                ms,
                file,
                lane_off,
                past_lease: false,
            },
        )
        .and_then(|expected| {
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

/// Plan 41 L3, the cap: a lone node, lease 12 s, lock TTL 3 s, one metadata
/// write holds the store 20 s — past what is left of the root lease, which
/// the stuck core cannot renew. The lane renews the node's own grant up to
/// the lease's usable end (`own_renewals > 0`) and no further: the grant
/// lapses there, the owner is fenced (the application sees `EIO` after it)
/// and the fence lifts with the lock; tree, file and fsck as in
/// `lock-stall-lone-node`.
pub fn lock_store_stall_past_lease(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-store-stall-past-lease";
    const LOCK_TTL_MS: u64 = 3_000;
    const STALL_MS: u64 = 20_000;
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let file = root.path().join("store-stall");
    let mut c = Client::new(root.path(), "lone", &env.direct_endpoint, &backend)?
        .with_write_mode("back")
        .with_env("CONSTELLATION_LOCKS", "cluster")
        .with_env("CONSTELLATION_LOCK_TTL_MS", &LOCK_TTL_MS.to_string())
        .with_env("CONSTELLATION_LEASE_TTL_MS", "12000")
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
    eprintln!("    {NAME}: a {STALL_MS} ms store stall, lease 12 s");
    let result = lock_lapse_lone_run(
        NAME,
        &c,
        LoneStall::Store {
            ms: STALL_MS,
            file,
            lane_off: false,
            past_lease: true,
        },
    )
    .and_then(|expected| {
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
    result
}

/// Plan 41 L4: a lock held at a healthy owner survives a stall of the
/// *holder's* store. Two nodes, a 3 s lock TTL, no backup: B holds an
/// exclusive `flock` (and writes under it) while A's client blocks on the
/// same lock and A creates files in a loop (the log B tails: B's core has
/// store writes to make). One ordinary metadata write of B's holds B's store
/// 10 s (`CONSTELLATION_FAULT_STORE_STALL_FILE`), longer than a grant's
/// window; B's core waits behind it. B's lane renews the grant at A
/// meanwhile: B keeps the lock (its write under it answers `OK`, nothing is
/// fenced or lost, `lane.remote_ok > 0`), A gets it only after B's unlock.
/// `CONSTELLATION_HARNESS_HOLDER_LANE=off` runs the daemons with
/// `CONSTELLATION_LOCK_LANE=off` and expects the lapse instead (B's write
/// answers `EIO`): the in-scenario fail-before.
pub fn lock_store_stall_holder(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-store-stall-holder";
    const LOCK_TTL_MS: u64 = 3_000;
    const STALL_MS: u64 = 10_000;
    const UNDER: &[u8] = b"B-UNDER-LOCK";
    const AFTER: &[u8] = b"B-AFTER-STALL";
    let lane_off = std::env::var("CONSTELLATION_HARNESS_HOLDER_LANE").as_deref() == Ok("off");
    let ttl = LOCK_TTL_MS.to_string();
    let stall_dir = tempfile::tempdir()?;
    let file = stall_dir.path().join("store-stall");
    let file_str = file.display().to_string();
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b"],
        &[
            ("CONSTELLATION_LOCK_TTL_MS", &ttl),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"),
            ("CONSTELLATION_FAULT_STORE_STALL_FILE", &file_str),
            (
                "CONSTELLATION_LOCK_LANE",
                if lane_off { "off" } else { "on" },
            ),
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
        std::fs::write(a.mnt.join("g"), b"initial")?;
        eventually("the file visible on b", Duration::from_secs(30), || {
            anyhow::ensure!(std::fs::read(b.mnt.join("g"))? == b"initial");
            Ok(())
        })?;
        let mut wb = open_rw(&b.mnt.join("g"))?;
        flock(&wb, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("B's lock: {e}"))?;
        write_at(&mut wb, 0, UNDER).context("B's write under its lock")?;
        // A's client waits for the same lock; A also creates files, so
        // B tails a log that moves.
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel::<Duration>();
        let a_wait = {
            let path = a.mnt.join("g");
            std::thread::spawn(move || -> Result<()> {
                let f = open_rw(&path)?;
                let t0 = Instant::now();
                flock(&f, libc::LOCK_EX).map_err(|e| anyhow::anyhow!("A's lock: {e}"))?;
                let _ = tx.send(t0.elapsed());
                let _ = flock(&f, libc::LOCK_UN);
                Ok(())
            })
        };
        let a_files = {
            let (dir, stop) = (a.mnt.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let _ = std::fs::write(dir.join(format!("busy-{i}")), b"x");
                    i += 1;
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
        };
        std::thread::sleep(Duration::from_millis(1_000));
        // B's store stalls: the next ordinary write of B's holds the lock.
        let pid = b.pid().context("b has no daemon")?;
        let tmp = file.with_extension("tmp");
        std::fs::write(&tmp, format!("{STALL_MS} once pid={pid}"))?;
        std::fs::rename(&tmp, &file)?;
        let trigger = {
            let path = b.mnt.join("stall-trigger");
            std::thread::spawn(move || std::fs::write(path, b"x"))
        };
        std::thread::sleep(Duration::from_millis(STALL_MS + 1_500));
        anyhow::ensure!(
            !file.exists(),
            "no metadata write of B's took the {STALL_MS} ms store stall"
        );
        let early = rx.try_recv().ok();
        let after = write_at(&mut wb, 0, AFTER);
        let locks = locks_of(b)?;
        let after_eio = after.as_ref().err().and_then(Code::from_os_error) == Some(Code::Io);
        eprintln!(
            "    {NAME}: after a {STALL_MS} ms stall of B's store: B's write {after:?}, A granted \
             {early:?} (before B's unlock); B's locks: {locks}"
        );
        if lane_off {
            anyhow::ensure!(
                after_eio,
                "with the lane off B's grant should have lapsed under a {STALL_MS} ms stall: its \
                 write answered {after:?}; locks: {locks}"
            );
        } else {
            anyhow::ensure!(
                after.is_ok() && early.is_none(),
                "B lost the lock to its own store stall (write {after:?}) or A was granted it \
                 while B held it ({early:?}); locks: {locks}"
            );
            anyhow::ensure!(
                locks["lost"].as_u64().unwrap_or(0) == 0
                    && locks["owners_fenced"].as_u64().unwrap_or(0) == 0
                    && locks["lane"]["remote_ok"].as_u64().unwrap_or(0) > 0,
                "B's lane did not carry its grant at the owner (remote_ok > 0, nothing lost or \
                 fenced expected): {locks}"
            );
        }
        // B lets go (the close of a lapsed one may answer EIO: the lane-off
        // arm); A then gets the lock.
        let _ = flock(&wb, libc::LOCK_UN);
        drop(wb);
        let granted = match early {
            Some(d) => d,
            None => rx
                .recv_timeout(Duration::from_secs(20))
                .context("A never got the lock after B's unlock")?,
        };
        eprintln!("    {NAME}: A's lock came {granted:?} after it asked");
        stop.store(true, Ordering::Relaxed);
        let _ = trigger.join();
        let _ = a_files.join();
        a_wait
            .join()
            .map_err(|_| anyhow::anyhow!("A's waiter panicked"))??;
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

/// Load campaign §5.3, finding F1 in the real daemon: a fencing token
/// checked before the op waits for the store's writer lock, and not again
/// at the write, lets a write admitted under a grant land after the grant
/// ended. A lone node; `A` (a thread with its own open file description)
/// holds `flock` on `refs/main.lock`, `B` waits for it. `A`'s
/// `rename(refs/tmp-a, refs/main)` passes the check, then its write holds
/// for 10 s (the store-stall hook, armed for `A`'s task only), during
/// which the daemon is frozen 6 s — past the 3 s grant's window. Then `A`
/// resumes: its rename must not land under the ended grant (ADR-25: an
/// operation admitted before a lapse and delayed past it never lands on
/// it). Plan 43 C7-F F-c: it answers `EIO` (refused) or, the grant having
/// been asked for back from the node itself and admitted, `OK` from a
/// fresh token (the status shows the late renewal admitted and the park
/// resumed); `B` takes the lock once `A` lets go and renames its own ref,
/// and `refs/main` holds `B`'s, also after a remount; fsck finds nothing.
/// Before F1's fix (a re-check under the writer lock) `A`'s rename answers
/// OK with no late renewal behind it.
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
            // Plan 43 C7-F F-c: the rename was refused as lapsed at the
            // writer lock, and sent again under a fresh token once the
            // node's own late renewal was admitted: it landed under a grant
            // the owner proved nobody else was given, and B (queued behind
            // A's lock) renames after it. F1 is a rename that lands past
            // the end of a grant nobody renewed.
            Ok(()) => {
                let l = locks_of(&c)?;
                // F1's own evidence first: the token check at the write
                // refused the rename's first send (its window had ended);
                // only a re-send under the admitted grant may land. The
                // late-renewal counters alone are this node's whole history
                // and would pass with F1's re-check gone.
                anyhow::ensure!(
                    l["token_rejections_at_write"].as_u64().unwrap_or(0) > 0
                        && l["late_admitted"].as_u64().unwrap_or(0) > 0
                        && l["lapse_parks_resumed"].as_u64().unwrap_or(0) > 0,
                    "F1: A's rename passed its fencing-token check, waited {took:?} for the writer \
                     lock past its grant's end, and answered OK without the write-time token check \
                     refusing it and the grant having been asked for back and admitted: a write \
                     under an ended grant landed; locks {l}"
                );
            }
            Err(e) => bail!("A's rename answered {e}, expected EIO (fenced) or OK (re-admitted)"),
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
    /// One of the core's own metadata commits holds the store's writer lock
    /// this long (`CONSTELLATION_FAULT_STORE_STALL_FILE` at `file`,
    /// `core-only`): the core's step is stuck, and every FUSE writer waits
    /// behind it, finding 08's shape. Plan 41: with the
    /// lock lane (the default) it must not lapse the grant (`lane_off`
    /// false: no `EIO`, nothing fenced, the lane's own renewals counted);
    /// with `CONSTELLATION_LOCK_LANE=off` (`lane_off`: the in-scenario
    /// fail-before, which proves the fault point effective) it must.
    Store {
        ms: u64,
        file: PathBuf,
        lane_off: bool,
        /// The stall outlasts the root lease (the stuck core cannot renew it):
        /// the lane's renewals end at the lease's usable end and the grant
        /// lapses there (`lock-store-stall-past-lease`).
        past_lease: bool,
    },
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
            LoneStall::Store { ms, file, .. } => {
                // Whole, by a rename (a prefix would stall for less).
                let tmp = file.with_extension("tmp");
                // The core's own commit is the slow one (`core-only`): the stall
                // reaches the core whatever the FUSE writers have to write.
                std::fs::write(&tmp, format!("{ms} once core-only"))?;
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
    // A freeze past the window lapses the grant by construction, and the
    // node asks itself for it back (plan 43 C7-F F-c): the write waited
    // for that and answers OK. A store stall may not lapse it, or the
    // verdict may come late (only OK or EIO are right then).
    anyhow::ensure!(
        if freeze {
            after.is_ok()
        } else {
            after_eio || after.is_ok()
        },
        "the write after the {} answered {after:?}, expected {}; locks: {fence}",
        if freeze { "freeze" } else { "store stall" },
        if freeze { "OK" } else { "OK or EIO" }
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
        let resumed = fence["lapse_parks_resumed"].as_u64().unwrap_or(0);
        anyhow::ensure!(
            resumed > 0 && fenced_ops == 0,
            "the daemon was frozen {frozen:?} past a {}-ms lock window: expected the grant asked \
             for back and its parked I/O resumed (lapse_parks_resumed > 0) with nothing fenced: \
             {fence}",
            fence["margin_ms"]
        );
        for (what, v) in &outcomes {
            if let Some((n, r)) = v.iter().find(|(_, r)| r.is_err()) {
                bail!("{what} {n} answered {r:?} after a freeze: the I/O should have waited");
            }
        }
    } else if let LoneStall::Store {
        ms,
        lane_off,
        past_lease,
        ..
    } = &stall
    {
        let eios: usize = outcomes
            .values()
            .map(|v| v.iter().filter(|(_, r)| *r == Err(Code::Io)).count())
            .sum();
        let owners_fenced = fence["owners_fenced"].as_u64().unwrap_or(0);
        let own = fence["lane"]["own_renewals"].as_u64().unwrap_or(0);
        eprintln!(
            "    {name}: the grant {} during the {ms} ms store stall ({fenced_ops} fenced ops, \
             {eios} EIO, {owners_fenced} owners fenced, lane own renewals {own})",
            if after_eio { "lapsed" } else { "held" }
        );
        if *lane_off {
            // The code before plan 41: the stuck core renews nothing, the
            // grant lapses under a stall past its window (the 3 s ttl). Plan
            // 43's lapse parking keeps the owner from being fenced (its
            // I/O parks for the stall and then answers OK), so the lapse is
            // read from the parking: the ops parked for most of the stall.
            let parks = fence["lapse_parks"].as_u64().unwrap_or(0);
            let parked_ms = fence["lapse_park_ms_max"].as_u64().unwrap_or(0);
            anyhow::ensure!(
                after_eio || fenced_ops > 0 || eios > 0 || (parks > 0 && parked_ms >= ms / 3),
                "with CONSTELLATION_LOCK_LANE=off a {ms} ms store stall lapsed nothing (lapse \
                 parks {parks}, longest {parked_ms} ms): the fault point did not reach the core \
                 (or the scenario is vacuous); locks: {fence}"
            );
            anyhow::ensure!(
                fence["lane"]["own_renewals"].as_u64().unwrap_or(0) == 0,
                "the lane renewed with the lane off: {fence}"
            );
        } else if *past_lease {
            // The stall outlasts the lease the stuck core cannot renew: the lane
            // renews up to the lease's usable end and the grant lapses there
            // (never later: the cap), the owner is fenced and the application
            // sees `EIO` until the lock is gone.
            // (Plan 43's lapse parking keeps the owner from being fenced: the
            // lapse shows as I/O parked for the rest of the stall.)
            let parks = fence["lapse_parks"].as_u64().unwrap_or(0);
            let parked_ms = fence["lapse_park_ms_max"].as_u64().unwrap_or(0);
            anyhow::ensure!(
                after_eio || fenced_ops > 0 || eios > 0 || (parks > 0 && parked_ms >= ms / 4),
                "a {ms} ms store stall outlasted the lease and the grant never lapsed (lapse \
                 parks {parks}, longest {parked_ms} ms): the cap was not enforced (or the stall \
                 did not reach the core); locks: {fence}"
            );
            anyhow::ensure!(
                own > 0,
                "the lane never renewed the node's own grant before the lease's end \
                 (lane.own_renewals {own}); locks: {fence}"
            );
        } else {
            // The lane renews meanwhile: no op answers EIO, nothing is fenced,
            // the lock is never lost, and the lane's own renewals are counted.
            anyhow::ensure!(
                !after_eio && eios == 0 && fenced_ops == 0 && owners_fenced == 0,
                "a {ms} ms store stall lapsed the lock with the lane on: {eios} EIO, \
                 {fenced_ops} fenced ops, {owners_fenced} owners fenced; locks: {fence}"
            );
            anyhow::ensure!(
                fence["lost"].as_u64().unwrap_or(0) == 0 && own > 0,
                "the lane did not renew the node's own grant during a {ms} ms store stall \
                 (lane.own_renewals {own}, lost {}); locks: {fence}",
                fence["lost"]
            );
            // The signal the lane-off arm shows: no op parked for a lapsed
            // grant (the lane kept the window open through the stall).
            let parks = fence["lapse_parks"].as_u64().unwrap_or(0);
            anyhow::ensure!(
                parks == 0 && fence["late_admitted"].as_u64().unwrap_or(0) == 0,
                "the grant lapsed during a {ms} ms store stall with the lane on: {parks} lapse \
                 parks, longest {} ms, {} late admissions; locks: {fence}",
                fence["lapse_park_ms_max"],
                fence["late_admitted"]
            );
        }
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
