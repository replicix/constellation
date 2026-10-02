//! What a FUSE session reports about its transport (plan 38 §5): the
//! transport it negotiated, its ring queue depth, the transport fallback
//! it took (if any) and why, and its zero-copy read count — per session
//! in [`SessionStats`], and process-wide in the counters a scrape needs to
//! stay monotonic across unmounts ([`fallback_counts`],
//! [`zero_copy_reads_total`]).
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
//! the build, the kernel's offer, the ring setup, and the handover pin.
//!
//! The vendored fuser does not hand back *why* its ring setup failed (it
//! logs it, once, during the handshake), and this chunk does not change
//! fuser: what the kernel offered is in [`NegotiatedInit::kernel_flags`],
//! which separates "the kernel never offered the ring" from "it offered
//! and the setup failed", and with the `io-uring` feature the host probe
//! (`fuser::uring_unavailable`) supplies the detail.

use crate::session::TransportPolicy;
use fuser::{InitFlags, NegotiatedInit, Transport};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Why a session that asked for the ring is served over `/dev/fuse`: the
/// `reason` label of `constellation_fuse_transport_fallbacks_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FallbackReason {
    /// The session can be handed to another process image, so it is
    /// pinned to `/dev/fuse` (plan 38 §3(e)) whatever the knob asked.
    HandoverCapable,
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

/// The downgrade a session took, or `None` when it got what it asked for
/// (module doc). Pure, so every rung is testable on a host that can grant
/// none of them: `asked` is the transport knob's policy *before* any
/// handover pin, `pinned` whether the session is handover-capable,
/// `feature` whether this build has the ring, `init` what the handshake
/// recorded (`None` for a session that never ran one) and `negotiated`
/// what the session is served over.
pub(crate) fn classify(
    asked: TransportPolicy,
    pinned: bool,
    feature: bool,
    init: Option<&NegotiatedInit>,
    negotiated: Transport,
) -> Option<FallbackReason> {
    if asked != TransportPolicy::Auto || !negotiated.is_dev_fuse() {
        return None;
    }
    Some(if pinned {
        FallbackReason::HandoverCapable
    } else if !feature {
        FallbackReason::NoIoUringFeature
    } else if init.is_some_and(|i| {
        !InitFlags::from_bits_retain(i.kernel_flags).contains(InitFlags::FUSE_OVER_IO_URING)
    }) {
        FallbackReason::KernelNotOffered
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
/// (`constellation_fuse_zero_copy_reads_total`). Nothing serves one before
/// plan 38 Z4, so this stays 0 until then.
static ZERO_COPY_READS: AtomicU64 = AtomicU64::new(0);

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
    zero_copy_reads: AtomicU64,
}

impl SessionStats {
    /// The stats of a session served over `negotiated`, recording — and
    /// counting, once — the fallback [`classify`] finds.
    pub(crate) fn at_handshake(
        asked: TransportPolicy,
        pinned: bool,
        init: Option<&NegotiatedInit>,
        negotiated: Transport,
        uring_queue_depth: usize,
    ) -> Self {
        let feature = cfg!(feature = "io-uring");
        let reason = classify(asked, pinned, feature, init, negotiated);
        Self::with_fallback(negotiated, uring_queue_depth, reason)
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
            zero_copy_reads: AtomicU64::new(0),
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
        self.zero_copy_reads.load(Relaxed)
    }

    /// Count one zero-copy read (plan 38 Z4's read path).
    pub fn count_zero_copy_read(&self) {
        self.zero_copy_reads.fetch_add(1, Relaxed);
        ZERO_COPY_READS.fetch_add(1, Relaxed);
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

    #[test]
    fn every_rung_of_the_ladder_names_its_own_reason() {
        use FallbackReason::*;
        use TransportPolicy::{Auto, DevFuse};
        let dev = Transport::DevFuse;
        let (on, off) = (Some(init(true)), Some(init(false)));
        // Never asked, or granted: nothing to record.
        assert_eq!(classify(DevFuse, false, true, off.as_ref(), dev), None);
        assert_eq!(classify(DevFuse, true, false, None, dev), None);
        assert_eq!(
            classify(Auto, false, true, on.as_ref(), Transport::Uring),
            None
        );
        // The pin wins over every other reason: it is why the ring was
        // never asked for at all.
        assert_eq!(
            classify(Auto, true, true, on.as_ref(), dev),
            Some(HandoverCapable)
        );
        assert_eq!(
            classify(Auto, false, false, on.as_ref(), dev),
            Some(NoIoUringFeature)
        );
        assert_eq!(
            classify(Auto, false, true, off.as_ref(), dev),
            Some(KernelNotOffered)
        );
        assert_eq!(
            classify(Auto, false, true, on.as_ref(), dev),
            Some(RingSetupFailed)
        );
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
        let stats = SessionStats::at_handshake(
            TransportPolicy::Auto,
            true,
            Some(&init(true)),
            Transport::DevFuse,
            8,
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
}
