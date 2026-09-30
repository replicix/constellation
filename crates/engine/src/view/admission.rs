//! Per-view admission (plan 31 §6.3's backpressure barrier, §9.10's
//! [`super::ViewQos`]): a view's own limits on ops in flight and on staged
//! write bytes, so one view sharing an engine with others cannot starve
//! them.
//!
//! An unlimited view (both limits `None`, every view before plan 31 C4c)
//! holds no admission state at all: `admit` is one branch on an `Option`
//! and returns a guard that does nothing — no atomic, no lock.
//!
//! A limited view's in-flight gate is an atomic counter with a
//! compare-and-swap fast path; only an op that finds the view full takes
//! the gate's mutex and waits on its condvar, woken by the next op to
//! leave. Past its deadline — the op's own (`OpCtx::deadline`), else
//! `CONSTELLATION_VIEW_ADMISSION_WAIT_MS` (default 30 s) — the op answers
//! `Code::Again`; a cancelled op answers `Code::Intr`.
//!
//! **What is never held back.** `flush` and `release` (a close must not
//! wait behind the writes it would end), the lock ops (a blocked
//! `F_SETLKW` defers; an unlock must reach the waiter it frees), and
//! `sync_view` (unmount). Holding one of those back behind a full view
//! could wait for itself.
//!
//! The staging limit is checked where a write starts, as a wait for the
//! view's own write sessions to flush (`StagingBudget::wait_below`), not
//! as `ENOSPC` in the middle of a write; a write is charged its whole
//! length (an overwrite of already-staged bytes is charged too — simple
//! and conservative). The node-wide staging budget keeps refusing
//! exactly as before.

use crate::staging::StagingBudget;
use constellation_types::Code;
use constellation_vfs::{OpCtx, OpKind};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// How long a held-back op waits by default
/// (`CONSTELLATION_VIEW_ADMISSION_WAIT_MS`).
fn default_wait() -> Duration {
    static WAIT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *WAIT.get_or_init(|| {
        Duration::from_millis(
            std::env::var("CONSTELLATION_VIEW_ADMISSION_WAIT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30_000),
        )
    })
}

/// Ops never held back (see the module doc).
fn exempt(kind: OpKind) -> bool {
    matches!(
        kind,
        OpKind::Flush
            | OpKind::Release
            | OpKind::LockTest
            | OpKind::LockAcquire
            | OpKind::LockRelease
            | OpKind::SyncView
    )
}

struct Gate {
    max: u32,
    inflight: AtomicU32,
    waiters: AtomicU32,
    lock: Mutex<()>,
    left: Condvar,
}

impl Gate {
    fn try_enter(&self) -> bool {
        let mut cur = self.inflight.load(Ordering::SeqCst);
        while cur < self.max {
            match self.inflight.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(now) => cur = now,
            }
        }
        false
    }

    fn leave(&self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) > 0 {
            let _held = self.lock.lock().unwrap();
            self.left.notify_one();
        }
    }
}

/// A view's admission state.
#[derive(Default)]
pub(crate) struct Admission {
    gate: Option<Gate>,
    /// The view's own staging count ([`StagingBudget::child`]).
    staging: Option<Arc<StagingBudget>>,
}

/// An admitted op: leaves the gate when dropped (after the op answered).
pub(crate) struct Admitted<'a>(Option<&'a Gate>);

impl Admitted<'_> {
    /// The op defers (plan 31 §6.3): it stays admitted past this guard's
    /// scope, until whoever completes it calls [`Admission::leave_deferred`]
    /// with what this returned (whether a slot is held).
    pub(crate) fn defer(self) -> bool {
        let held = self.0.is_some();
        std::mem::forget(self);
        held
    }
}

impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        if let Some(gate) = self.0 {
            gate.leave();
        }
    }
}

impl Admission {
    /// Limits for one view; `node_staging` is the node's staging budget
    /// a staging limit is a share of.
    pub(crate) fn new(qos: &super::ViewQos, node_staging: &Arc<StagingBudget>) -> Self {
        Self {
            gate: qos.max_inflight_ops.map(|max| Gate {
                max: max.max(1),
                inflight: AtomicU32::new(0),
                waiters: AtomicU32::new(0),
                lock: Mutex::new(()),
                left: Condvar::new(),
            }),
            staging: qos
                .max_staging_bytes
                .map(|limit| StagingBudget::child(node_staging, limit)),
        }
    }

    /// The staging budget this view's write sessions reserve against,
    /// when it has its own.
    pub(crate) fn staging(&self) -> Option<&Arc<StagingBudget>> {
        self.staging.as_ref()
    }

    fn deadline(cx: &OpCtx<'_>) -> Instant {
        cx.deadline
            .unwrap_or_else(|| Instant::now() + default_wait())
    }

    /// Let `cx`'s op in, waiting while the view is full.
    pub(crate) fn admit(&self, cx: &OpCtx<'_>) -> Result<Admitted<'_>, Code> {
        let Some(gate) = &self.gate else {
            return Ok(Admitted(None));
        };
        if exempt(cx.kind) {
            return Ok(Admitted(None));
        }
        if gate.try_enter() {
            return Ok(Admitted(Some(gate)));
        }
        constellation_vfs::watch::stage("view admission (in-flight limit)");
        let deadline = Self::deadline(cx);
        let mut held = gate.lock.lock().unwrap();
        gate.waiters.fetch_add(1, Ordering::SeqCst);
        let result = loop {
            if gate.try_enter() {
                break Ok(Admitted(Some(gate)));
            }
            if cx.cancelled() {
                break Err(Code::Intr);
            }
            let now = Instant::now();
            if now >= deadline {
                break Err(Code::Again);
            }
            // Bounded so a cancellation is noticed without a wake-up.
            let slice = (deadline - now).min(Duration::from_millis(100));
            held = gate.left.wait_timeout(held, slice).unwrap().0;
        };
        gate.waiters.fetch_sub(1, Ordering::SeqCst);
        result
    }

    /// A deferred op ([`Admitted::defer`]) has completed: give its slot
    /// back if it held one.
    pub(crate) fn leave_deferred(&self, held: bool) {
        if let (true, Some(gate)) = (held, &self.gate) {
            gate.leave();
        }
    }

    /// Before a write of `bytes`: wait while the view's staged bytes
    /// would exceed its limit.
    pub(crate) fn admit_staging(&self, cx: &OpCtx<'_>, bytes: u64) -> Result<(), Code> {
        let Some(staging) = &self.staging else {
            return Ok(());
        };
        if staging.used().saturating_add(bytes) <= staging.budget() {
            return Ok(());
        }
        constellation_vfs::watch::stage("view admission (staging limit)");
        if staging.wait_below(bytes, Self::deadline(cx)) {
            Ok(())
        } else {
            Err(Code::Again)
        }
    }
}
