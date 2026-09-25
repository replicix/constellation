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
//! Not supported: interrupting a blocked lock wait (fuser 0.18 delivers no
//! `FUSE_INTERRUPT`, so Ctrl-C of a blocked `flock`/`F_SETLKW` returns
//! only once the lock is granted), and POSIX deadlock detection
//! (`EDEADLK`) — two owners waiting on each other wait forever, as they do
//! with `flock`.

use crate::fusefs::SyncRequest;
use anyhow::{bail, Result};
use constellation_authority::{
    LockAnswer, LockOutcome, LockRenewEntry, LockRenewResult, LockTestAnswer, LockTestOutcome,
};
use constellation_fs_core::Ino;
use constellation_meta::locks::{Grant, GrantId, LocalLock, LocalOutcome, LockMode};
use constellation_meta::{JournalPos, Meta, Position, ReadKey};
use constellation_net::{LockOutcomeWire, LockRenewResultWire, LockRenewWire, LockTestOutcomeWire};
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
    pub inval: Option<crate::kernel_inval::InodeInvalidator>,
}

impl ClusterLocks {
    /// Whether I/O on `ino` must be refused (`EIO`): local locks under a
    /// lapsed grant. One relaxed atomic load when no local lock exists.
    pub fn fenced(&self, ino: Ino) -> bool {
        self.meta.locks().fenced(ino, now_ms())
    }

    /// `getlk`: the conflicting lock, as `(start, end, type, pid)`;
    /// `F_UNLCK` when there is none. `typ == F_UNLCK` asks about any lock
    /// (a read test). Another node's grant answers as a whole-file lock
    /// with pid 0.
    pub fn test(
        &self,
        ino: Ino,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
    ) -> (u64, u64, i32, u32) {
        let write = typ == libc::F_WRLCK;
        let kind = |w: bool| if w { libc::F_WRLCK } else { libc::F_RDLCK };
        match self
            .meta
            .locks()
            .local_test(ino, owner, write, start, end, now_ms())
        {
            LocalOutcome::Conflict(l) => (l.start, l.end.min(OFFSET_MAX), kind(l.write), l.pid),
            LocalOutcome::Done => (0, 0, libc::F_UNLCK, 0),
            LocalOutcome::NeedGrant(mode) => {
                let (reply, answer) = tokio::sync::oneshot::channel();
                if self
                    .tx
                    .send(SyncRequest::LockTest { ino, mode, reply })
                    .is_err()
                {
                    return (0, 0, libc::F_UNLCK, 0);
                }
                match answer.blocking_recv() {
                    Ok(LockTestAnswer::Held { mode, .. }) => {
                        (0, OFFSET_MAX, kind(mode == LockMode::Exclusive), 0)
                    }
                    _ => (0, 0, libc::F_UNLCK, 0),
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
    /// fill it. fuser 0.18 has no `FUSE_INTERRUPT`: such a wait cannot be
    /// cancelled from the application (see the module doc).
    pub fn lock(
        self: &Arc<Self>,
        ino: Ino,
        lock: LocalLock,
        sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        fn answer(r: Result<(), i32>, reply: fuser::ReplyEmpty) {
            match r {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(fuser::Errno::from_i32(e)),
            }
        }
        if !sleep {
            answer(self.set(ino, lock, false), reply);
            return;
        }
        let this = self.clone();
        let spawned = std::thread::Builder::new()
            .name("lock-wait".into())
            .spawn(move || answer(this.set(ino, lock, true), reply));
        if let Err(error) = spawned {
            // The reply went with the closure; dropping it answers EIO.
            tracing::warn!(%error, ino, "could not start a lock-wait thread");
        }
    }

    fn set(&self, ino: Ino, lock: LocalLock, sleep: bool) -> Result<(), i32> {
        let mut rounds = 0u32;
        loop {
            match self.meta.locks().local_set(ino, lock, now_ms()) {
                LocalOutcome::Done => return Ok(()),
                LocalOutcome::Conflict(_) => {
                    if !sleep {
                        return Err(libc::EAGAIN);
                    }
                    std::thread::sleep(LOCAL_POLL);
                }
                LocalOutcome::NeedGrant(mode) => {
                    if !sleep && rounds >= TRY_ROUNDS {
                        return Err(libc::EAGAIN);
                    }
                    if rounds >= 2 {
                        // A grant that keeps being overtaken by recalls:
                        // back off a little instead of spinning on the
                        // sequencer.
                        std::thread::sleep(Duration::from_millis(10 * u64::from(rounds.min(20))));
                    }
                    rounds += 1;
                    let (reply, answer) = tokio::sync::oneshot::channel();
                    self.tx
                        .send(SyncRequest::Lock {
                            ino,
                            mode,
                            blocking: sleep,
                            reply,
                        })
                        .map_err(|_| libc::EIO)?;
                    match answer.blocking_recv().map_err(|_| libc::EIO)? {
                        LockAnswer::Granted { position } => self.granted(ino, &position),
                        LockAnswer::WouldBlock => {
                            if !sleep {
                                return Err(libc::EAGAIN);
                            }
                            // The owner parks blocking requests, so this
                            // is a race with a recall; ask again shortly.
                            std::thread::sleep(LOCAL_POLL);
                        }
                        LockAnswer::Unavailable => return Err(libc::ENOLCK),
                    }
                }
            }
        }
    }

    /// A grant arrived: what the previous holder wrote under its lock is
    /// at or before `position`. Wait for the replica to reach it, then
    /// drop the kernel's pages and attributes of the file, which may
    /// predate it.
    fn granted(&self, ino: Ino, position: &Position) {
        let waited = self.meta.session_wait_at(&[ReadKey::Ino(ino)], position);
        tracing::debug!(target: "constellation::locks", ino, ?position, ?waited, "lock granted");
        if let Some(inval) = &self.inval {
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
