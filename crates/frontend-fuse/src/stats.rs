//! What a FUSE session reports about its transport (plan 38 §5): the
//! transport it negotiated, its ring queue depth, the transport fallback
//! it took (if any) and why, its zero-copy read count and how many blocking
//! lock requests its ring's lock-wait budget served as non-blocking — per
//! session in [`SessionStats`], and process-wide in the counters a scrape
//! needs to stay monotonic across unmounts ([`fallback_counts`],
//! [`zero_copy_reads_total`], [`lock_wait_downgrades_total`]).
//!
//! # A fallback is recorded once, at the handshake
//!
//! Plan 38 §2.4 asks that every downgrade be logged once and be visible in
//! `node.status`, not merely in the log. The transport is fixed for the
//! life of a connection, so the only moment a downgrade can happen is the
//! handshake, and [`classify`] runs exactly then: a session that asked for
//! the ladder ([`TransportPolicy::Auto`]) and was served over `/dev/fuse`
//! records one [`TransportFallback`] and bumps the process-wide counter by
//! exactly one. A session that never asked (the `dev-fuse` default) took
//! no fallback and records nothing.
//!
//! # Reasons are a closed set
//!
//! [`FallbackReason`] is the counter's `reason` label, so it is a fixed
//! handful of names rather than the error text: the text (an
//! `io_uring_setup` errno, the value of `fuse.enable_uring`) goes in
//! [`TransportFallback::detail`], which `node.status` carries and the
//! metric does not. Each name is a rung of §2.4's ladder that can refuse:
//! the build, the kernel's offer, the cluster-lock rule of plan 38 Z2c,
//! the handover pin, and the ring setup.
//!
//! # The first rung that refused
//!
//! A session reports the rung that would have refused it first, in the
//! ladder's order — build, kernel, cluster locks, handover pin, setup —
//! so a policy reason (`cluster_locks`, `handover_capable`) is named only
//! where the ring would otherwise have been granted. A session resumed by
//! `daemon --upgrade` on a kernel that never offered the ring therefore
//! keeps reporting `kernel_not_offered` (its handoff carries the original
//! `FUSE_INIT`), not the pin it was resumed under.
//!
//! # Lock-wait downgrades
//!
//! A ring queue lends at most `depth - 1` of its entries to blocking lock
//! requests; the vendored fuser serves a further one as non-blocking
//! (`ENOLCK` if contended) and tells [`LockWaitCounter`] so. Each such
//! downgrade is an error a `/dev/fuse` mount would not have returned, so
//! it is counted per session and process-wide
//! (`constellation_fuse_lock_wait_downgrades_total`).
//!
//! The vendored fuser does not hand back *why* its ring setup failed (it
//! logs it, once, during the handshake), and this chunk does not change
//! fuser: what the kernel offered is in [`NegotiatedInit::kernel_flags`],
//! which separates "the kernel never offered the ring" from "it offered
//! and the setup failed", and with the `io-uring` feature the host probe
//! (`fuser::uring_unavailable`) supplies the detail.

use crate::passthrough::{PassthroughState, PassthroughStatus};
use crate::session::TransportPolicy;
use fuser::{InitFlags, NegotiatedInit, Transport};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Why a session that asked for the ring is served over `/dev/fuse`: the
/// `reason` label of `constellation_fuse_transport_fallbacks_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FallbackReason {
    /// The session can be handed to another process image, so it is
    /// pinned to `/dev/fuse` (plan 38 §3(e)) whatever the knob asked.
    HandoverCapable,
    /// `auto` keeps a mount whose frontend forwards locks to the cluster
    /// on `/dev/fuse` (plan 38 Z2c; [`TransportPolicy`]'s doc):
    /// `--fuse-transport uring` is the opt-in.
    ClusterLocks,
    /// This binary was built without the `io-uring` feature.
    NoIoUringFeature,
    /// The kernel's `FUSE_INIT` did not offer `FUSE_OVER_IO_URING`
    /// (kernel < 6.14, or `fuse.enable_uring=N`).
    KernelNotOffered,
    /// The kernel offered the ring and creating it failed (a seccomp
    /// `EPERM` on `io_uring_setup`, a refused reservation or
    /// registration); fuser logged the error during the handshake.
    RingSetupFailed,
}

impl FallbackReason {
    /// The label value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HandoverCapable => "handover_capable",
            Self::ClusterLocks => "cluster_locks",
            Self::NoIoUringFeature => "no_io_uring_feature",
            Self::KernelNotOffered => "kernel_not_offered",
            Self::RingSetupFailed => "ring_setup_failed",
        }
    }
}

/// One transport downgrade, as `node.status.fuse.mounts[].last_fallback`
/// reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportFallback {
    /// What the session asked for.
    pub from: Transport,
    /// What it got.
    pub to: Transport,
    pub reason: FallbackReason,
    /// Free text: what refused, as precisely as this side knows.
    pub detail: String,
    /// When the handshake recorded it, Unix milliseconds.
    pub at_unix_ms: u64,
}

/// What a session's handshake settled, as [`SessionStats::at_handshake`]
/// records it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Handshake<'a> {
    /// The transport knob's policy *before* any handover pin.
    pub asked: TransportPolicy,
    /// The session is handover-capable (pinned to `/dev/fuse`).
    pub pinned: bool,
    /// `auto` kept it off the ring for its cluster locks.
    pub held_back_for_locks: bool,
    /// What the handshake recorded (`None` for one that never ran).
    pub init: Option<&'a NegotiatedInit>,
    /// What the session is served over.
    pub negotiated: Transport,
    /// The queue depth a ring would have / has.
    pub uring_queue_depth: usize,
}

/// The downgrade a session took, or `None` when it got what it asked for:
/// the first rung that refused, in the ladder's order (module doc). Pure,
/// so every rung is testable on a host that can grant none of them:
/// `feature` is whether this build has the ring.
pub(crate) fn classify(h: &Handshake<'_>, feature: bool) -> Option<FallbackReason> {
    if !h.asked.asks_for_ring() || !h.negotiated.is_dev_fuse() {
        return None;
    }
    let offered = |i: &NegotiatedInit| {
        InitFlags::from_bits_retain(i.kernel_flags).contains(InitFlags::FUSE_OVER_IO_URING)
    };
    Some(if !feature {
        FallbackReason::NoIoUringFeature
    } else if h.init.is_some_and(|i| !offered(i)) {
        FallbackReason::KernelNotOffered
    } else if h.held_back_for_locks {
        FallbackReason::ClusterLocks
    } else if h.pinned {
        FallbackReason::HandoverCapable
    } else {
        FallbackReason::RingSetupFailed
    })
}

/// The free-text detail of a fallback for `reason`.
fn detail(reason: FallbackReason) -> String {
    let base = match reason {
        FallbackReason::HandoverCapable => {
            "the session can be handed to another process image, so it stays on /dev/fuse \
             (a ring session cannot be detached)"
        }
        FallbackReason::ClusterLocks => {
            "the mount forwards locks to the cluster, and transport auto keeps such a mount on \
             /dev/fuse (a blocked lock wait holds a ring entry; --fuse-transport uring opts in \
             and answers contended waits past the queue's budget with ENOLCK)"
        }
        FallbackReason::NoIoUringFeature => "this build has no io-uring feature",
        FallbackReason::KernelNotOffered => {
            "the kernel did not offer FUSE_OVER_IO_URING (kernel < 6.14 or fuse.enable_uring=N)"
        }
        FallbackReason::RingSetupFailed => {
            "the kernel offered the ring but creating it failed (see the daemon log)"
        }
    };
    match reason {
        FallbackReason::KernelNotOffered | FallbackReason::RingSetupFailed => match probe() {
            Some(why) => format!("{base}: {why}"),
            None => base.to_string(),
        },
        _ => base.to_string(),
    }
}

/// The host probe's answer, where the build has one.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn probe() -> Option<String> {
    fuser::uring_unavailable()
}

#[cfg(not(all(feature = "io-uring", target_os = "linux")))]
fn probe() -> Option<String> {
    None
}

/// Every fallback this process took, by (from, to, reason): the
/// `constellation_fuse_transport_fallbacks_total` counter. Process-wide,
/// not per session, so a scrape never sees it step back when the mount
/// that took one goes away.
static FALLBACKS: Mutex<BTreeMap<(&'static str, &'static str, &'static str), u64>> =
    Mutex::new(BTreeMap::new());

/// Zero-copy reads served by every session of this process
/// (`constellation_fuse_zero_copy_reads_total`): reads the kernel handed
/// over as registered pages and that were answered with one `READ_FIXED`
/// from a chunk file (plan 38 Z4b).
static ZERO_COPY_READS: AtomicU64 = AtomicU64::new(0);

/// One session's zero-copy reads: shared by the session's filesystem,
/// whose read replies count, and its [`SessionStats`], which report.
#[derive(Debug, Clone, Default)]
pub struct ZeroCopyCounter(Arc<AtomicU64>);

impl ZeroCopyCounter {
    /// Count one zero-copy read, on the session and in the process total.
    pub fn count(&self) {
        self.0.fetch_add(1, Relaxed);
        ZERO_COPY_READS.fetch_add(1, Relaxed);
    }

    /// Reads counted so far.
    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

/// Count one fallback (plan 38 Z3b's passthrough downgrades, which happen
/// per mount and per open rather than at the handshake: `from` is
/// `passthrough`, `to` the session's transport, `reason` one of
/// [`crate::passthrough::reason`]'s names).
pub(crate) fn count_fallback(from: &'static str, to: &'static str, reason: &'static str) {
    *FALLBACKS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry((from, to, reason))
        .or_default() += 1;
}

/// `(from, to, reason, count)` for every fallback this process took.
pub fn fallback_counts() -> Vec<(&'static str, &'static str, &'static str, u64)> {
    FALLBACKS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|(&(from, to, reason), &n)| (from, to, reason, n))
        .collect()
}

/// Zero-copy reads served by this process.
pub fn zero_copy_reads_total() -> u64 {
    ZERO_COPY_READS.load(Relaxed)
}

/// Blocking lock requests every ring session of this process served as
/// non-blocking (`constellation_fuse_lock_wait_downgrades_total`).
static LOCK_WAIT_DOWNGRADES: AtomicU64 = AtomicU64::new(0);

/// Lock-wait downgrades of every session of this process (module doc).
pub fn lock_wait_downgrades_total() -> u64 {
    LOCK_WAIT_DOWNGRADES.load(Relaxed)
}

/// One session's lock-wait downgrades (module doc), counted by the hook
/// the vendored fuser calls on its ring threads.
///
/// The hook runs on a ring thread, which must never block: it does two
/// relaxed atomic adds and one `tracing::debug!` (no lock of ours, no
/// I/O beyond what the installed subscriber does for an enabled debug
/// event — off by default), and nothing else.
#[derive(Debug, Clone, Default)]
pub struct LockWaitCounter(Arc<AtomicU64>);

impl LockWaitCounter {
    /// The hook a ring session's `fuser::Config` carries: this session's
    /// count and the process-wide total, one each per downgrade. Logged at
    /// debug only: under a burst one line per refused waiter is the spam
    /// plan 38 §2.4 rules out, and the counters are what an operator reads.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub(crate) fn hook(&self) -> fuser::LockWaitDowngrades {
        let n = self.0.clone();
        fuser::LockWaitDowngrades::new(move || {
            n.fetch_add(1, Relaxed);
            LOCK_WAIT_DOWNGRADES.fetch_add(1, Relaxed);
            tracing::debug!(
                "a ring queue's lock-wait budget served a blocking lock as non-blocking"
            );
        })
    }

    /// Count one downgrade by hand (what the hook does).
    pub fn count(&self) {
        self.0.fetch_add(1, Relaxed);
        LOCK_WAIT_DOWNGRADES.fetch_add(1, Relaxed);
    }

    /// Downgrades counted so far.
    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One session's transport state (module doc). Built by the session at
/// the handshake; the host keeps a clone for `node.status`.
#[derive(Debug)]
pub struct SessionStats {
    transport: Transport,
    uring_queue_depth: u32,
    last_fallback: Option<TransportFallback>,
    zero_copy_reads: ZeroCopyCounter,
    /// The session's passthrough state (plan 38 Z3b), read live.
    passthrough: Option<Arc<PassthroughState>>,
    lock_wait_downgrades: LockWaitCounter,
}

impl SessionStats {
    /// The stats of a session whose handshake settled `h`, recording —
    /// and counting, once — the fallback [`classify`] finds.
    pub(crate) fn at_handshake(
        h: Handshake<'_>,
        lock_waits: LockWaitCounter,
        passthrough: Arc<PassthroughState>,
    ) -> Self {
        let reason = classify(&h, cfg!(feature = "io-uring"));
        let mut stats = Self::with_fallback(h.negotiated, h.uring_queue_depth, reason);
        stats.lock_wait_downgrades = lock_waits;
        stats.passthrough = Some(passthrough);
        stats
    }

    /// The session's passthrough (plan 38 Z3b): whether it registers
    /// backing files, how many handles the kernel serves from one now,
    /// why not. A `SessionStats` built without a session reports it off.
    pub fn passthrough(&self) -> PassthroughStatus {
        self.passthrough
            .as_ref()
            .map(|p| p.status())
            .unwrap_or_default()
    }

    fn with_fallback(
        negotiated: Transport,
        uring_queue_depth: usize,
        reason: Option<FallbackReason>,
    ) -> Self {
        let last_fallback = reason.map(|reason| {
            let fallback = TransportFallback {
                // `Auto` climbs to the ring; zero-copy is a property of a
                // ring session's reads (plan 38 Z4), not a transport this
                // policy asks for by name.
                from: Transport::Uring,
                to: negotiated,
                reason,
                detail: detail(reason),
                at_unix_ms: now_unix_ms(),
            };
            *FALLBACKS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry((fallback.from.name(), fallback.to.name(), reason.as_str()))
                .or_default() += 1;
            fallback
        });
        Self {
            transport: negotiated,
            // A queue depth describes ring queues; a `/dev/fuse` session
            // has none.
            uring_queue_depth: if negotiated.is_dev_fuse() {
                0
            } else {
                uring_queue_depth.clamp(1, u32::MAX as usize) as u32
            },
            last_fallback,
            zero_copy_reads: ZeroCopyCounter::default(),
            passthrough: None,
            lock_wait_downgrades: LockWaitCounter::default(),
        }
    }

    /// The transport the session is served over.
    pub fn transport(&self) -> Transport {
        self.transport
    }

    /// Ring entries per kernel queue; 0 on `/dev/fuse`.
    pub fn uring_queue_depth(&self) -> u32 {
        self.uring_queue_depth
    }

    /// The downgrade this session took at its handshake, if any.
    pub fn last_fallback(&self) -> Option<&TransportFallback> {
        self.last_fallback.as_ref()
    }

    /// Zero-copy reads this session served.
    pub fn zero_copy_reads(&self) -> u64 {
        self.zero_copy_reads.get()
    }

    /// Report the reads `counter` counts (the session's filesystem's).
    pub(crate) fn with_zero_copy_reads(mut self, counter: ZeroCopyCounter) -> Self {
        self.zero_copy_reads = counter;
        self
    }

    /// Blocking lock requests this session's ring served as non-blocking
    /// (module doc); always 0 on `/dev/fuse`.
    pub fn lock_wait_downgrades(&self) -> u64 {
        self.lock_wait_downgrades.get()
    }

    /// Count one zero-copy read (plan 38 Z4's read path).
    pub fn count_zero_copy_read(&self) {
        self.zero_copy_reads.count();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init(offered: bool) -> NegotiatedInit {
        let kernel_flags = if offered {
            InitFlags::FUSE_OVER_IO_URING.bits()
        } else {
            0
        };
        NegotiatedInit {
            kernel_major: 7,
            kernel_minor: 41,
            proto_major: 7,
            proto_minor: 41,
            kernel_flags,
            flags: 0,
            max_readahead: 0,
            max_write: 0,
            max_background: 0,
            congestion_threshold: 0,
            time_gran_ns: 1,
            max_pages: 0,
            max_stack_depth: 0,
            transport: Transport::DevFuse,
        }
    }

    fn hs(
        asked: TransportPolicy,
        pinned: bool,
        held_back_for_locks: bool,
        init: Option<&NegotiatedInit>,
        negotiated: Transport,
    ) -> Handshake<'_> {
        Handshake {
            asked,
            pinned,
            held_back_for_locks,
            init,
            negotiated,
            uring_queue_depth: 8,
        }
    }

    #[test]
    fn every_rung_of_the_ladder_names_its_own_reason() {
        use FallbackReason::*;
        use TransportPolicy::{Auto, DevFuse, Uring};
        let dev = Transport::DevFuse;
        let (on, off) = (Some(init(true)), Some(init(false)));
        let c = |h: Handshake<'_>, feature: bool| classify(&h, feature);
        // Never asked, or granted: nothing to record.
        assert_eq!(c(hs(DevFuse, false, false, off.as_ref(), dev), true), None);
        assert_eq!(c(hs(DevFuse, true, false, None, dev), false), None);
        for asked in [Auto, Uring] {
            assert_eq!(
                c(hs(asked, false, false, on.as_ref(), Transport::Uring), true),
                None
            );
            assert_eq!(
                c(hs(asked, false, false, on.as_ref(), dev), false),
                Some(NoIoUringFeature)
            );
            assert_eq!(
                c(hs(asked, false, false, off.as_ref(), dev), true),
                Some(KernelNotOffered)
            );
            assert_eq!(
                c(hs(asked, false, false, on.as_ref(), dev), true),
                Some(RingSetupFailed)
            );
        }
        // The policy rungs, named only where the ring was on offer.
        assert_eq!(
            c(hs(Auto, false, true, on.as_ref(), dev), true),
            Some(ClusterLocks)
        );
        assert_eq!(
            c(hs(Auto, true, false, on.as_ref(), dev), true),
            Some(HandoverCapable)
        );
        // A pinned mount with cluster locks under `auto` would not have
        // had the ring anyway: the earlier rung is the one named.
        assert_eq!(
            c(hs(Auto, true, true, on.as_ref(), dev), true),
            Some(ClusterLocks)
        );
    }

    /// The Z2b review's finding: a session `daemon --upgrade` resumed
    /// (pinned, carrying the original `FUSE_INIT`) keeps the reason its
    /// first mount had — the kernel or the build — and reports the pin
    /// only where the kernel offered the ring.
    #[test]
    fn a_resumed_session_keeps_its_original_first_rung() {
        use TransportPolicy::Auto;
        let dev = Transport::DevFuse;
        let off = init(false);
        let on = init(true);
        assert_eq!(
            classify(&hs(Auto, true, false, Some(&off), dev), true),
            Some(FallbackReason::KernelNotOffered)
        );
        assert_eq!(
            classify(&hs(Auto, true, false, Some(&on), dev), false),
            Some(FallbackReason::NoIoUringFeature)
        );
        assert_eq!(
            classify(&hs(Auto, true, false, Some(&on), dev), true),
            Some(FallbackReason::HandoverCapable)
        );
    }

    /// Plan 38 §2.4's kernel < 6.14 rung, which no host here can run: an
    /// `InitFlags` without `FUSE_OVER_IO_URING` (every other bit a real
    /// 6.x kernel offers set) is `kernel_not_offered`, with the bit it is
    /// `ring_setup_failed`, and the classification reads nothing else of
    /// the negotiation.
    #[test]
    fn an_init_without_the_ring_bit_is_kernel_not_offered() {
        let mut old = init(false);
        old.kernel_flags = (InitFlags::all() - InitFlags::FUSE_OVER_IO_URING).bits();
        old.kernel_minor = 40;
        let h = hs(
            TransportPolicy::Auto,
            false,
            false,
            Some(&old),
            Transport::DevFuse,
        );
        assert_eq!(classify(&h, true), Some(FallbackReason::KernelNotOffered));
        let mut new = old;
        new.kernel_flags |= InitFlags::FUSE_OVER_IO_URING.bits();
        let h = Handshake {
            init: Some(&new),
            ..h
        };
        assert_eq!(classify(&h, true), Some(FallbackReason::RingSetupFailed));
    }

    /// Plan 38 §2.4: a recorded fallback is visible on its session and
    /// counted exactly once, by its (from, to, reason). (`handover_capable`
    /// because no other test in this binary records that key — the
    /// `auto` mount tests can record the others concurrently.)
    #[test]
    fn a_fallback_is_recorded_on_the_session_and_counted_once() {
        let key = ("uring", "dev_fuse", "handover_capable");
        let count = || {
            fallback_counts()
                .into_iter()
                .find(|&(f, t, r, _)| (f, t, r) == key)
                .map_or(0, |(.., n)| n)
        };
        let before = count();
        // The reason as `classify` names it (tested above), recorded: the
        // build's feature does not enter into this one.
        let stats = SessionStats::with_fallback(
            Transport::DevFuse,
            8,
            Some(FallbackReason::HandoverCapable),
        );
        let fallback = stats.last_fallback().expect("the fallback is recorded");
        assert_eq!(
            (fallback.from, fallback.to, fallback.reason),
            (
                Transport::Uring,
                Transport::DevFuse,
                FallbackReason::HandoverCapable
            )
        );
        assert!(fallback.at_unix_ms > 0);
        assert!(fallback.detail.contains("handed to another process"));
        assert_eq!(count(), before + 1, "counted exactly once");
        assert_eq!(stats.uring_queue_depth(), 0, "no ring queues on /dev/fuse");

        let granted = SessionStats::with_fallback(Transport::Uring, 16, None);
        assert!(granted.last_fallback().is_none());
        assert_eq!(granted.uring_queue_depth(), 16);
        let zc = zero_copy_reads_total();
        granted.count_zero_copy_read();
        assert_eq!(granted.zero_copy_reads(), 1);
        assert!(zero_copy_reads_total() > zc);
    }

    /// Plan 38 Z2c: a lock-wait downgrade counts on its session and in
    /// the process-wide total, once each.
    #[test]
    fn a_lock_wait_downgrade_counts_on_the_session_and_in_the_total() {
        let counter = LockWaitCounter::default();
        let offered = init(true);
        let stats = SessionStats::at_handshake(
            hs(
                TransportPolicy::Uring,
                false,
                false,
                Some(&offered),
                Transport::Uring,
            ),
            counter.clone(),
            crate::passthrough::PassthroughState::new(crate::passthrough::PassthroughWish::Off(
                crate::passthrough::reason::DISABLED,
            )),
        );
        let before = lock_wait_downgrades_total();
        assert_eq!(stats.lock_wait_downgrades(), 0);
        counter.count();
        counter.count();
        assert_eq!(stats.lock_wait_downgrades(), 2);
        assert!(lock_wait_downgrades_total() >= before + 2);
    }
}
