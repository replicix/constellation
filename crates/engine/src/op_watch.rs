//! The engine's side of the frontend's request watchdog (plan 31 C3
//! seam; C4 generalises it into `OpWatch`, plan 31 §6.8).
//!
//! The watchdog itself — the registry of requests in flight, the stall
//! monitor, `status`'s view of it — is the FUSE adapter's
//! (`cli/src/fuse_watch.rs`). What the engine needs from it is small: an
//! engine wait that a frontend request runs into (a lock grant's core
//! reply, the session wait after it, the kernel invalidation) names
//! itself as the request's current *stage*, so a request that stalls
//! there is reported with the wait it is stuck in.
//!
//! So the thread-local "request this thread is handling" lives here: the
//! frontend sets it when a request starts ([`set_current`]) and clears it
//! when it ends ([`clear_current`]); [`stage`] notes a stage on it, and is
//! a no-op on a thread handling no request (the sync task's own flushes).
//! A wait the engine moves to a thread of its own (a blocking lock, see
//! `locks::ClusterLocks::lock`) carries the request along with
//! [`current`] + [`set_current`], so the waiter's stages still name it.

use std::cell::RefCell;
use std::sync::Arc;

/// A frontend request in flight, as the engine sees it.
pub trait OpStage: Send + Sync {
    /// Note what the request is about to wait on.
    fn stage(&self, stage: &'static str);
}

thread_local! {
    /// The request the current thread is handling (see the module doc).
    static CURRENT: RefCell<Option<Arc<dyn OpStage>>> = const { RefCell::new(None) };
}

/// Make `op` the request the current thread is handling (`None`: none).
pub fn set_current(op: Option<Arc<dyn OpStage>>) {
    CURRENT.with(|c| *c.borrow_mut() = op);
}

/// The request the current thread is handling, to hand to another thread.
pub fn current() -> Option<Arc<dyn OpStage>> {
    CURRENT.with(|c| c.borrow().clone())
}

/// Clear the current thread's request if it is still `op` (a request that
/// ended after another one started on this thread leaves that one alone).
pub fn clear_current(op: &Arc<dyn OpStage>) {
    CURRENT.with(|c| {
        let mut cur = c.borrow_mut();
        if cur
            .as_ref()
            .is_some_and(|e| std::ptr::addr_eq(Arc::as_ptr(e), Arc::as_ptr(op)))
        {
            *cur = None;
        }
    });
}

/// Note what the current thread's request is about to wait on. A no-op
/// on a thread handling no request.
pub fn stage(stage: &'static str) {
    CURRENT.with(|c| {
        if let Some(op) = c.borrow().as_ref() {
            op.stage(stage);
        }
    });
}
