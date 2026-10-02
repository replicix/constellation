//! Plan 30 §M14: `--locks local|cluster` — cross-node `flock`/`fcntl`.
//!
//! The protocol lives in `constellation_authority::core` (its `locks`
//! module doc: grants leased from the owning sequencer, recalls, renewals,
//! fencing) and the tables in `constellation_meta::locks` (shared by the
//! core and the FUSE threads). This module is the daemon's side of it:
//! the mount option, the FUSE-facing request loop ([`ClusterLocks`]), the
//! flush the core asks for before a recalled grant is released
//! ([`LockFlushers`]), and the conversions between the core's messages
//! and their P2P wire form.
//!
//! Plan 31: the FUSE reply plumbing (`fuser::ReplyEmpty`/`ReplyLock`,
//! the `F_RDLCK`/`F_WRLCK`/`F_UNLCK` lock types) is the frontend's
//! (`constellation-frontend-fuse`); the view's `lock_test`/`lock_acquire`/
//! `lock_release` ops (`view::ops`) call in here. [`ClusterLocks::lock`]
//! completes through a callback that completes the op's
//! `constellation_vfs::Responder`, and [`ClusterLocks::test`] speaks
//! `write: bool`.
//!
//! - `--locks local`: today's behaviour. The FUSE mount does not
//!   negotiate `FUSE_POSIX_LOCKS`/`FUSE_FLOCK_LOCKS`, so the kernel keeps
//!   every lock node-local and never asks the daemon. Two nodes can both
//!   "hold" an exclusive lock on the same file.
//! - `--locks cluster` (the default with P2P): a lock taken on one node
//!   excludes conflicting locks on every other node. The first lock on a
//!   file costs one round trip to its sequencer; the grant is then cached
//!   (an uncontended re-lock costs nothing) until another node wants a
//!   conflicting one. Data written under a lock is flushed through before
//!   the grant moves, and the next holder waits for it and drops its
//!   kernel cache of the file, so lock-protected read-modify-write works
//!   across nodes. A node whose grant lapsed (partitioned from the
//!   sequencer past the ttl) fails I/O on the files it holds locks on
//!   with `EIO` until they are unlocked (NFSv4's rule).
//! - Without P2P (`CONSTELLATION_P2P=off`, or an endpoint that could not
//!   start) the effective mode is `local`: the inbox is not a lock path.
//!   An explicit `--locks cluster` then fails the mount.
//! - `CONSTELLATION_LOCKS` supplies the default when the flag is absent.
//! - `CONSTELLATION_LOCK_TTL_MS` (default 5000): a grant's lifetime,
//!   renewed in the background while held.
//! - `CONSTELLATION_LOCK_CACHE_IDLE_MS` (default 30000): how long an
//!   unused cached grant is kept before it is released.
//!
//! Not supported: interrupting a blocked lock wait (the FUSE adapter wires
//! `FUSE_INTERRUPT` to the `fsync` family only, plan 39 §3.3, so Ctrl-C of
//! a blocked `flock`/`F_SETLKW` returns only once the lock is granted), and
//! POSIX deadlock detection
//! (`EDEADLK`) — two owners waiting on each other wait forever, as they do
//! with `flock`.

use crate::sync::SyncRequest;
use anyhow::{bail, Result};
use constellation_authority::{
    LockAnswer, LockOutcome, LockRenewEntry, LockRenewResult, LockTestAnswer, LockTestOutcome,
};
use constellation_fs_core::Ino;
use constellation_meta::locks::{Grant, GrantId, LocalLock, LocalOutcome, LockMode};
use constellation_meta::{JournalPos, Meta, Position, ReadKey};
use constellation_net::{LockOutcomeWire, LockRenewResultWire, LockRenewWire, LockTestOutcomeWire};
use constellation_types::Code;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// The error an explicit `--locks cluster` gets without P2P.
pub const NEEDS_P2P: &str = "cluster locks need P2P; use --locks local";

/// `--locks` (or `CONSTELLATION_LOCKS`): `Some(true)` cluster,
/// `Some(false)` local, `None` when neither says (cluster with P2P).
pub fn cluster_flag(flag: Option<&str>) -> Result<Option<bool>> {
    let env = std::env::var("CONSTELLATION_LOCKS").ok();
    let Some(raw) = flag.or(env.as_deref()) else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => Ok(None),
        "local" => Ok(Some(false)),
        "cluster" => Ok(Some(true)),
        other => bail!("invalid --locks {other:?} (expected local or cluster)"),
    }
}

/// The effective mode: cluster unless asked for local or P2P is off; an
/// explicit cluster without P2P is an error.
pub fn cluster_effective(flag: Option<bool>, p2p: bool) -> Result<bool> {
    match flag {
        Some(true) if !p2p => bail!(NEEDS_P2P),
        Some(v) => Ok(v),
        None => Ok(p2p),
    }
}

fn env_ms(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// `CONSTELLATION_LOCK_TTL_MS` (default 5000).
pub fn lock_ttl_ms(default: u64) -> u64 {
    env_ms("CONSTELLATION_LOCK_TTL_MS", default)
}

/// `CONSTELLATION_LOCK_CACHE_IDLE_MS` (default 30000).
pub fn lock_cache_idle_ms(default: u64) -> u64 {
    env_ms("CONSTELLATION_LOCK_CACHE_IDLE_MS", default)
}

fn now_ms() -> i64 {
    constellation_store_s3::lease::now_unix_ms()
}

/// The largest offset the kernel accepts in a lock reply (`OFFSET_MAX`);
/// anything past it is answered `EIO` by `convert_fuse_file_lock`.
const OFFSET_MAX: u64 = i64::MAX as u64;

/// How often a blocked request re-checks a conflicting local lock (local
/// conflicts wake nobody: the other owner's unlock is a plain table
/// update).
const LOCAL_POLL: Duration = Duration::from_millis(15);

/// How long a granted lock waits for the kernel to drop its cache of the
/// file before it is used anyway (the invalidation thread can be held up
/// by a notification waiting on an unrelated FUSE request).
const INVAL_WAIT: Duration = Duration::from_secs(1);

/// Non-blocking requests give up with `EAGAIN` after this many grant
/// round trips that did not end in a lock (each one lost to a recall that
/// overtook it: another node wants the file).
const TRY_ROUNDS: u32 = 4;

/// The FUSE side of cluster locks, shared by a view's FUSE threads and
/// the threads that wait for `F_SETLKW`/`flock` (`None` in
/// `SyncHandle::locks` is `--locks local`).
pub struct ClusterLocks {
    pub meta: Arc<Meta>,
    pub tx: tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    /// The frontends' kernel caches of a file, dropped after a grant
    /// (`crate::kernel_inval`); `None` with kernel invalidation off.
    pub inval: Option<crate::kernel_inval::InodeInvalidator>,
}

impl ClusterLocks {
    /// Whether I/O on `ino` must be refused (`EIO`): local locks under a
    /// lapsed grant. One relaxed atomic load when no local lock exists.
    pub fn fenced(&self, ino: Ino) -> bool {
        self.meta.locks().fenced(ino, now_ms())
    }

    /// Whether `ino`'s dirty data must be discarded rather than
    /// published (`Some(fenced)`; see `LockTables::take_discard`).
    pub fn take_discard(&self, ino: Ino) -> Option<bool> {
        self.meta.locks().take_discard(ino, now_ms())
    }

    /// Dirty data of `ino` was discarded: every open file description of
    /// it reports `EIO` once, at its next close or `fsync`
    /// (`LockTables::note_discard`). Returns the event's sequence number.
    pub fn note_discard(&self, ino: Ino) -> u64 {
        self.meta.locks().note_discard(ino)
    }

    /// Drop the kernel's pages and attributes of `ino` (not waited for:
    /// callers are FUSE request handlers, which the notification may be
    /// queued behind).
    pub fn invalidate(&self, ino: Ino) {
        if let Some(inval) = &self.inval {
            inval.invalidate(ino);
        }
    }

    /// `getlk`: the conflicting lock, as `Some((start, end, write,
    /// pid))`; `None` when there is none. `write: false` asks about any
    /// lock (a read test, or `F_UNLCK`). Another node's grant answers as a
    /// whole-file lock with pid 0.
    pub fn test(
        &self,
        ino: Ino,
        owner: u64,
        start: u64,
        end: u64,
        write: bool,
    ) -> Option<(u64, u64, bool, u32)> {
        match self
            .meta
            .locks()
            .local_test(ino, owner, write, start, end, now_ms())
        {
            LocalOutcome::Conflict(l) => Some((l.start, l.end.min(OFFSET_MAX), l.write, l.pid)),
            LocalOutcome::Done => None,
            LocalOutcome::NeedGrant(mode) => {
                constellation_vfs::watch::stage("lock test (core reply)");
                let (reply, answer) = tokio::sync::oneshot::channel();
                if self
                    .tx
                    .send(SyncRequest::LockTest { ino, mode, reply })
                    .is_err()
                {
                    return None;
                }
                match answer.blocking_recv() {
                    Ok(LockTestAnswer::Held { mode, .. }) => {
                        Some((0, OFFSET_MAX, mode == LockMode::Exclusive, 0))
                    }
                    _ => None,
                }
            }
        }
    }

    /// `setlk` with `F_UNLCK`: drop `owner`'s locks in the range.
    pub fn unlock(&self, ino: Ino, owner: u64, start: u64, end: u64) {
        if self
            .meta
            .locks()
            .local_unlock(ino, owner, start, end, now_ms())
        {
            self.idle(ino);
        }
    }

    /// A close (`flush`) or a `flock` release: drop every lock of `owner`
    /// on `ino` (POSIX: closing any descriptor drops the process's locks
    /// on the file). Returns whether no local lock is left on `ino`; the
    /// caller then calls [`Self::idle`] once the file's data is flushed.
    pub fn drop_owner(&self, ino: Ino, owner: u64) -> bool {
        self.meta.locks().local_release_owner(ino, owner, now_ms())
    }

    /// No local lock is left on `ino`: if its grant was recalled, the
    /// core releases it now (flushing the file first). Nobody waits.
    pub fn idle(&self, ino: Ino) {
        if self.meta.locks().held(ino).is_some_and(|h| h.recalled) {
            let _ = self.tx.send(SyncRequest::LockIdle { ino });
        }
    }

    /// `setlk` with `F_RDLCK`/`F_WRLCK`. A blocking request (`sleep`)
    /// waits on a thread of its own, never on the FUSE worker: a
    /// contended `flock` may wait for as long as another node holds the
    /// file, and a worker pinned per waiter would starve the mount. Not
    /// the runtime's blocking pool either: it is small (4 threads on one
    /// CPU) and the release a waiter waits for may itself need it (the
    /// driver's `Action::LockFlush`), so waiters must never be able to
    /// fill it. The FUSE adapter does not wire `FUSE_INTERRUPT` to it: such
    /// a wait cannot be cancelled from the application (see the module doc).
    ///
    /// `done` answers the request (the view's completes the op's
    /// responder), on this thread or on the waiter's.
    pub fn lock<F>(self: &Arc<Self>, ino: Ino, lock: LocalLock, sleep: bool, done: F)
    where
        F: FnOnce(Result<(), Code>) + Send + 'static,
    {
        if !sleep {
            done(self.set(ino, lock, false));
            return;
        }
        let this = self.clone();
        // The request's watchdog entry moves with the reply: the waiter
        // thread's stages name it (`constellation_vfs::watch::current`,
        // `Current::adopt`).
        let op = constellation_vfs::watch::current();
        let spawned = std::thread::Builder::new()
            .name("lock-wait".into())
            .spawn(move || {
                if let Some(op) = op {
                    op.adopt();
                }
                done(this.set(ino, lock, true))
            });
        if let Err(error) = spawned {
            // The reply went with the closure; dropping it answers EIO.
            tracing::warn!(%error, ino, "could not start a lock-wait thread");
        }
    }

    /// [`Self::lock`] on the calling thread, blocking or not: for a
    /// frontend that cannot answer an op from another thread
    /// (`constellation_vfs::FrontendCaps::deferrable`).
    pub fn lock_here(&self, ino: Ino, lock: LocalLock, sleep: bool) -> Result<(), Code> {
        self.set(ino, lock, sleep)
    }

    fn set(&self, ino: Ino, lock: LocalLock, sleep: bool) -> Result<(), Code> {
        let mut rounds = 0u32;
        loop {
            match self.meta.locks().local_set(ino, lock, now_ms()) {
                LocalOutcome::Done => return Ok(()),
                LocalOutcome::Conflict(_) => {
                    if !sleep {
                        return Err(Code::Again);
                    }
                    std::thread::sleep(LOCAL_POLL);
                }
                LocalOutcome::NeedGrant(mode) => {
                    if !sleep && rounds >= TRY_ROUNDS {
                        return Err(Code::Again);
                    }
                    if rounds >= 2 {
                        // A grant that keeps being overtaken by recalls:
                        // back off a little instead of spinning on the
                        // sequencer.
                        std::thread::sleep(Duration::from_millis(10 * u64::from(rounds.min(20))));
                    }
                    rounds += 1;
                    constellation_vfs::watch::stage("lock grant (core reply)");
                    let (reply, answer) = tokio::sync::oneshot::channel();
                    self.tx
                        .send(SyncRequest::Lock {
                            ino,
                            mode,
                            blocking: sleep,
                            reply,
                        })
                        .map_err(|_| Code::Io)?;
                    match answer.blocking_recv().map_err(|_| Code::Io)? {
                        LockAnswer::Granted { position } => self.granted(ino, &position),
                        LockAnswer::WouldBlock => {
                            if !sleep {
                                return Err(Code::Again);
                            }
                            // The owner parks blocking requests, so this
                            // is a race with a recall; ask again shortly.
                            std::thread::sleep(LOCAL_POLL);
                        }
                        LockAnswer::Unavailable => return Err(Code::NoLock),
                    }
                }
            }
        }
    }

    /// A grant arrived: what the previous holder wrote under its lock is
    /// at or before `position` — to this file and to every other one
    /// (the owner joins the releaser's frontier in). Wait for the replica
    /// to reach it, then drop the kernel's pages and attributes of the
    /// file, which may predate it.
    ///
    /// `position` also becomes this session's `observed` watermark, so a
    /// read of any *other* file waits for it too, however the wait for
    /// the locked file itself ended (a speculation that covers it, or the
    /// session budget): the application reads the refs next to the lock
    /// file, not only the lock file (EC2 campaign 4 B-1).
    fn granted(&self, ino: Ino, position: &Position) {
        self.meta.session().raise_observed(*position);
        constellation_vfs::watch::stage("session wait after a lock grant");
        let waited = self.meta.session_wait_at(&[ReadKey::Ino(ino)], position);
        tracing::debug!(target: "constellation::locks", ino, ?position, ?waited, "lock granted");
        match waited {
            constellation_meta::SessionWait::Waited(d) => {
                self.meta.locks().with_stats(|s| {
                    s.grants_waited += 1;
                    s.grant_wait_ms_total += d.as_millis() as u64;
                });
            }
            constellation_meta::SessionWait::TimedOut(d) => {
                // The guarantee the grant carries — the holder reads what
                // the previous holders wrote — does not hold for this
                // turn: the replica has not reached the floor, and the
                // reads under the lock answer from what it has. Counted
                // (`locks.grants_degraded`) and said out loud, every time.
                self.meta.locks().with_stats(|s| {
                    s.grants_waited += 1;
                    s.grant_wait_ms_total += d.as_millis() as u64;
                    s.grants_degraded += 1;
                });
                tracing::warn!(
                    target: "constellation::locks",
                    ino,
                    floor = ?position,
                    applied = ?self.meta.session().applied(),
                    waited = ?d,
                    "a lock grant's floor was not reached within the session budget: the reads \
                     under this grant are degraded (they may not see what the previous holder wrote)"
                );
            }
            _ => {}
        }
        if let Some(inval) = &self.inval {
            constellation_vfs::watch::stage("kernel invalidation after a lock grant");
            if !inval.invalidate_and_wait(ino, INVAL_WAIT) {
                tracing::debug!(
                    target: "constellation::locks",
                    ino,
                    "kernel cache invalidation after a lock grant timed out"
                );
            }
        }
    }
}

/// One view's flush for a recalled lock grant (`Action::LockFlush`):
/// what `fsync` guarantees, for one inode.
pub trait LockFlush: Send + Sync {
    fn flush_for_lock(&self, ino: Ino) -> bool;
}

/// Every mounted view's flush, for the driver's `Action::LockFlush`: a
/// file's dirty data lives in the write state of whichever view wrote it.
/// Weak: a view's filesystem goes away with its FUSE session.
#[derive(Default)]
pub struct LockFlushers {
    views: Mutex<Vec<(u64, Weak<dyn LockFlush>)>>,
}

impl LockFlushers {
    pub fn register(&self, id: u64, view: Weak<dyn LockFlush>) {
        let mut v = self.views.lock().unwrap();
        v.retain(|(_, w)| w.strong_count() > 0);
        v.push((id, view));
    }

    pub fn unregister(&self, id: u64) {
        self.views.lock().unwrap().retain(|(i, _)| *i != id);
    }

    /// Flush `ino` in every live view (blocking; run it off the runtime).
    /// No live view: nothing in memory to flush.
    pub fn flush(&self, ino: Ino) -> bool {
        let views: Vec<Arc<dyn LockFlush>> = self
            .views
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, w)| w.upgrade())
            .collect();
        views.iter().all(|v| v.flush_for_lock(ino))
    }
}

/// The driver's hook for `Action::LockFlush` (blocking).
pub type LockFlushHook = Arc<dyn Fn(Ino) -> bool + Send + Sync>;

// ---------------------------------------------------------------- wire

pub fn exclusive(mode: LockMode) -> bool {
    mode == LockMode::Exclusive
}

pub fn mode_of(exclusive: bool) -> LockMode {
    if exclusive {
        LockMode::Exclusive
    } else {
        LockMode::Shared
    }
}

pub fn grant_wire(id: GrantId) -> (u64, u64) {
    (id.node, id.seq)
}

pub fn grant_of((node, seq): (u64, u64)) -> GrantId {
    GrantId { node, seq }
}

pub fn outcome_wire(o: &LockOutcome) -> LockOutcomeWire {
    match o {
        LockOutcome::Granted {
            id,
            mode,
            ttl_ms,
            position,
        } => LockOutcomeWire::Granted {
            grant: grant_wire(*id),
            exclusive: exclusive(*mode),
            ttl_ms: *ttl_ms,
            position_seq: position.seq,
            position_pending: position.pending.map(|p| (p.epoch, p.jseq)),
            position_streams: position.streams_wire(),
        },
        LockOutcome::Waiting { retry_ms } => LockOutcomeWire::Waiting {
            retry_ms: *retry_ms,
        },
        LockOutcome::WouldBlock => LockOutcomeWire::WouldBlock,
        LockOutcome::NotOwner { owner } => LockOutcomeWire::NotOwner { owner: *owner },
        LockOutcome::Busy => LockOutcomeWire::Busy,
    }
}

pub fn outcome_of(w: LockOutcomeWire) -> LockOutcome {
    match w {
        LockOutcomeWire::Granted {
            grant,
            exclusive,
            ttl_ms,
            position_seq,
            position_pending,
            position_streams,
        } => LockOutcome::Granted {
            id: grant_of(grant),
            mode: mode_of(exclusive),
            ttl_ms,
            position: Position {
                seq: position_seq,
                pending: position_pending.map(|(epoch, jseq)| JournalPos { epoch, jseq }),
                streams: Default::default(),
            }
            .with_streams_wire(&position_streams),
        },
        LockOutcomeWire::Waiting { retry_ms } => LockOutcome::Waiting { retry_ms },
        LockOutcomeWire::WouldBlock => LockOutcome::WouldBlock,
        LockOutcomeWire::NotOwner { owner } => LockOutcome::NotOwner { owner },
        LockOutcomeWire::Busy => LockOutcome::Busy,
    }
}

pub fn renew_entries_wire(entries: &[LockRenewEntry]) -> Vec<LockRenewWire> {
    entries
        .iter()
        .map(|e| LockRenewWire {
            ino: e.ino,
            grant: grant_wire(e.grant),
            exclusive: exclusive(e.mode),
        })
        .collect()
}

pub fn renew_entries_of(entries: Vec<LockRenewWire>) -> Vec<LockRenewEntry> {
    entries
        .into_iter()
        .map(|e| LockRenewEntry {
            ino: e.ino,
            grant: grant_of(e.grant),
            mode: mode_of(e.exclusive),
        })
        .collect()
}

pub fn renew_results_wire(
    results: &[(Ino, GrantId, LockRenewResult)],
) -> Vec<(u64, (u64, u64), LockRenewResultWire)> {
    results
        .iter()
        .map(|(ino, id, r)| {
            let r = match *r {
                LockRenewResult::Ok {
                    ttl_ms,
                    recalled,
                    id,
                    mode,
                } => LockRenewResultWire::Ok {
                    ttl_ms,
                    recalled,
                    id: grant_wire(id),
                    exclusive: exclusive(mode),
                },
                LockRenewResult::Lost => LockRenewResultWire::Lost,
                LockRenewResult::NotOwner { owner } => LockRenewResultWire::NotOwner { owner },
            };
            (*ino, grant_wire(*id), r)
        })
        .collect()
}

pub fn renew_results_of(
    results: Vec<(u64, (u64, u64), LockRenewResultWire)>,
) -> Vec<(Ino, GrantId, LockRenewResult)> {
    results
        .into_iter()
        .map(|(ino, id, r)| {
            let r = match r {
                LockRenewResultWire::Ok {
                    ttl_ms,
                    recalled,
                    id,
                    exclusive,
                } => LockRenewResult::Ok {
                    ttl_ms,
                    recalled,
                    id: grant_of(id),
                    mode: mode_of(exclusive),
                },
                LockRenewResultWire::Lost => LockRenewResult::Lost,
                LockRenewResultWire::NotOwner { owner } => LockRenewResult::NotOwner { owner },
            };
            (ino, grant_of(id), r)
        })
        .collect()
}

pub fn test_outcome_wire(o: LockTestOutcome) -> LockTestOutcomeWire {
    match o {
        LockTestOutcome::Free => LockTestOutcomeWire::Free,
        LockTestOutcome::Held { node, mode } => LockTestOutcomeWire::Held {
            node,
            exclusive: exclusive(mode),
        },
        LockTestOutcome::NotOwner { owner } => LockTestOutcomeWire::NotOwner { owner },
    }
}

pub fn test_outcome_of(w: LockTestOutcomeWire) -> LockTestOutcome {
    match w {
        LockTestOutcomeWire::Free => LockTestOutcome::Free,
        LockTestOutcomeWire::Held { node, exclusive } => LockTestOutcome::Held {
            node,
            mode: mode_of(exclusive),
        },
        LockTestOutcomeWire::NotOwner { owner } => LockTestOutcome::NotOwner { owner },
    }
}

/// Grants handed over with a delegation or mirrored to a backup
/// (postcard; an undecodable batch is empty — the receiver's grace and
/// the holders' reclaims cover what it would have carried).
pub fn grants_wire(grants: &[Grant]) -> Vec<u8> {
    if grants.is_empty() {
        return Vec::new();
    }
    postcard::to_allocvec(grants).unwrap_or_default()
}

/// A lock floor on the wire (empty: none).
pub fn floor_wire(p: &constellation_meta::Position) -> Vec<u8> {
    if *p == constellation_meta::Position::ZERO {
        return Vec::new();
    }
    p.to_postcard()
}

pub fn floor_of(bytes: &[u8]) -> constellation_meta::Position {
    if bytes.is_empty() {
        return constellation_meta::Position::ZERO;
    }
    constellation_meta::Position::from_postcard(bytes)
}

pub fn grants_of(bytes: &[u8]) -> Vec<Grant> {
    if bytes.is_empty() {
        return Vec::new();
    }
    postcard::from_bytes(bytes).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse_and_need_p2p() {
        assert_eq!(cluster_flag(Some("local")).unwrap(), Some(false));
        assert_eq!(cluster_flag(Some("Cluster")).unwrap(), Some(true));
        assert!(cluster_flag(Some("global")).is_err());
        assert!(cluster_effective(None, true).unwrap());
        assert!(!cluster_effective(None, false).unwrap());
        assert!(!cluster_effective(Some(false), true).unwrap());
        let e = cluster_effective(Some(true), false).unwrap_err();
        assert!(e.to_string().contains("use --locks local"));
    }

    #[test]
    fn wire_forms_round_trip() {
        let position = Position {
            seq: 9,
            pending: Some(JournalPos { epoch: 3, jseq: 4 }),
            streams: Default::default(),
        };
        let granted = LockOutcome::Granted {
            id: GrantId { node: 2, seq: 7 },
            mode: LockMode::Exclusive,
            ttl_ms: 5000,
            position,
        };
        assert_eq!(outcome_of(outcome_wire(&granted)), granted);
        for o in [
            LockOutcome::Waiting { retry_ms: 3 },
            LockOutcome::WouldBlock,
            LockOutcome::NotOwner { owner: 4 },
            LockOutcome::Busy,
        ] {
            assert_eq!(outcome_of(outcome_wire(&o)), o);
        }
        let entries = vec![LockRenewEntry {
            ino: 5,
            grant: GrantId { node: 1, seq: 2 },
            mode: LockMode::Shared,
        }];
        assert_eq!(renew_entries_of(renew_entries_wire(&entries)), entries);
        let results = vec![
            (
                5,
                GrantId { node: 1, seq: 2 },
                LockRenewResult::Ok {
                    ttl_ms: 10,
                    recalled: true,
                    id: GrantId { node: 1, seq: 9 },
                    mode: LockMode::Exclusive,
                },
            ),
            (6, GrantId { node: 1, seq: 3 }, LockRenewResult::Lost),
            (
                7,
                GrantId { node: 1, seq: 4 },
                LockRenewResult::NotOwner { owner: 8 },
            ),
        ];
        assert_eq!(renew_results_of(renew_results_wire(&results)), results);
        for o in [
            LockTestOutcome::Free,
            LockTestOutcome::Held {
                node: 3,
                mode: LockMode::Shared,
            },
            LockTestOutcome::NotOwner { owner: 1 },
        ] {
            assert_eq!(test_outcome_of(test_outcome_wire(o)), o);
        }
        let grants = vec![Grant {
            id: GrantId { node: 1, seq: 2 },
            node: 3,
            ino: 4,
            mode: LockMode::Exclusive,
            until_ms: 99,
            recalled: false,
            gen: 0,
        }];
        assert_eq!(grants_of(&grants_wire(&grants)), grants);
        assert!(grants_of(&[]).is_empty());
    }
}
