//! `cancellation`: a [`CancelToken`] set before or while an op waits
//! completes it `Code::Intr`, and a wait that ends any other way (a
//! deadline) leaves nothing behind.
//!
//! The waiting op in the contract today is a blocking lock
//! (`lock_acquire` with `sleep`). The token exists for NFS, WinFsp and
//! control-client cancellation and for tests; **Linux FUSE never sets
//! it**, because fuser 0.18 delivers no `FUSE_INTERRUPT` (plan 31 §6.3's
//! known gap, carried forward on purpose): a blocked `F_SETLKW`/`flock` on
//! a Linux mount returns only once granted, Ctrl-C or not. A target
//! declares whether its waits honour the token
//! ([`super::Declared::cancellable_waits`]); one that does not skips this
//! group naming the gap.

use super::{must, refused, skip, Client, Env, Fx, TestResult};
use crate::ctx::{CancelToken, OpKind};
use crate::types::{Fh, Ino, LockKind, LockOwner, LockRange};
use constellation_types::Code;
use std::time::{Duration, Instant};

const GAP: &str = "the target does not honour CancelToken on waits (declared); a Linux FUSE \
                   frontend never has one set: fuser 0.18 delivers no FUSE_INTERRUPT (plan 31 §6.3)";

fn range() -> LockRange {
    LockRange {
        start: 0,
        end: i64::MAX as u64,
    }
}

fn lock(owner: u64) -> crate::LockSpec {
    Client::whole_file_lock(owner, LockKind::Write)
}

/// A file with its whole range locked by owner 1 through the returned
/// handle.
fn held(fx: &Fx) -> (Client, Ino, Fh) {
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x"));
    let ino = f.attr.ino;
    let holder = must("open", c.open_rw(ino));
    must("holder locks", c.try_lock(ino, holder.fh, lock(1)));
    (c, ino, holder.fh)
}

pub(super) fn cancelled_before_the_wait_is_interrupted(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if !fx.declared.cancellable_waits {
        skip!("{GAP}");
    }
    let (c, ino, holder) = held(&fx);
    let waiter = must("open", c.open_rw(ino));
    let token = CancelToken::new();
    token.cancel();
    // Already cancelled when the op starts: it must not wait for the
    // holder.
    let started = Instant::now();
    let pending = c.lock_wait_async(ino, waiter.fh, lock(2), Some(&token), None);
    refused("cancelled before waiting", pending.wait(), Code::Intr);
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "took {:?}",
        started.elapsed()
    );
    // Nothing was left queued: after the holder lets go, a third owner
    // takes the lock at once.
    must(
        "holder unlocks",
        c.unlock(ino, holder, LockOwner(1), range()),
    );
    let third = must("open", c.open_rw(ino));
    must("third owner locks", c.try_lock(ino, third.fh, lock(3)));
    must(
        "third unlocks",
        c.unlock(ino, third.fh, LockOwner(3), range()),
    );
    Ok(())
}

pub(super) fn cancelled_during_the_wait_is_interrupted(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if !fx.declared.cancellable_waits {
        skip!("{GAP}");
    }
    let (c, ino, holder) = held(&fx);
    let waiter = must("open", c.open_rw(ino));
    let token = CancelToken::new();
    let mut pending = if fx.caps.deferrable.contains(OpKind::LockAcquire) {
        let mut pending = c.lock_wait_async(ino, waiter.fh, lock(2), Some(&token), None);
        assert!(
            !pending.completes_within(Duration::from_millis(150)),
            "the wait ended while the holder still held the lock"
        );
        token.cancel();
        pending
    } else {
        // The wait parks this thread: cancel it from another one.
        std::thread::scope(|s| {
            s.spawn(|| {
                std::thread::sleep(Duration::from_millis(150));
                token.cancel();
            });
            let started = Instant::now();
            let pending = c.lock_wait_async(ino, waiter.fh, lock(2), Some(&token), None);
            assert!(
                started.elapsed() >= Duration::from_millis(100),
                "the wait ended while the holder still held the lock"
            );
            pending
        })
    };
    assert!(
        pending.completes_within(Duration::from_secs(20)),
        "a cancelled wait never completed"
    );
    let (result, _) = pending.outcome();
    assert_eq!(
        result,
        Some(Err(Code::Intr.into())),
        "a cancelled wait completes with EINTR"
    );
    // The interrupted waiter must not take the lock when it frees.
    must(
        "holder unlocks",
        c.unlock(ino, holder, LockOwner(1), range()),
    );
    std::thread::sleep(Duration::from_millis(50));
    let third = must("open", c.open_rw(ino));
    must("third owner locks", c.try_lock(ino, third.fh, lock(3)));
    Ok(())
}

pub(super) fn cancel_after_completion_is_harmless(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if !fx.declared.cancellable_waits {
        skip!("{GAP}");
    }
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x"));
    let ino = f.attr.ino;
    let a = must("open", c.open_rw(ino));
    let b = must("open", c.open_rw(ino));
    let token = CancelToken::new();
    // Uncontended: granted at once, so the token is never consulted.
    must(
        "granted",
        c.lock_wait_async(ino, a.fh, lock(1), Some(&token), None)
            .wait(),
    );
    token.cancel();
    std::thread::sleep(Duration::from_millis(50));
    // Cancelling afterwards changes nothing: the lock is still held.
    refused("still held", c.try_lock(ino, b.fh, lock(2)), Code::Again);
    must("unlock", c.unlock(ino, a.fh, LockOwner(1), range()));
    must("now free", c.try_lock(ino, b.fh, lock(2)));
    Ok(())
}

pub(super) fn a_timed_out_wait_leaves_no_waiter_behind(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if !fx.declared.cancellable_waits {
        skip!("{GAP}");
    }
    let (c, ino, holder) = held(&fx);
    let waiter = must("open", c.open_rw(ino));
    let deadline = Instant::now() + Duration::from_millis(200);
    let started = Instant::now();
    let pending = c.lock_wait_async(ino, waiter.fh, lock(2), None, Some(deadline));
    refused("past the deadline", pending.wait(), Code::TimedOut);
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "ended early: {:?}",
        started.elapsed()
    );
    must(
        "holder unlocks",
        c.unlock(ino, holder, LockOwner(1), range()),
    );
    std::thread::sleep(Duration::from_millis(50));
    let third = must("open", c.open_rw(ino));
    must("third owner locks", c.try_lock(ino, third.fh, lock(3)));
    Ok(())
}
