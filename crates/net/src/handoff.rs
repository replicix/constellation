//! Lease handoff state machine (DESIGN.md §4, roadmap M3.3).
//!
//! Without P2P a waiting writer gets the lease when the holder's idle
//! window elapses or its TTL expires. With a live peer the wait is one
//! round trip: the requester asks, the holder flushes and releases, and
//! the requester CAS-claims.
//!
//! **S3 remains the commit point.** The handoff message only tells the
//! requester that it is worth trying the CAS *now*; authority still comes
//! from the lease object. A lost, forged, or delayed message can
//! therefore only cost latency, never correctness — the requester's CAS
//! fails and it falls back to polling.
//!
//! The state machine is written against the [`Handoff`] trait rather
//! than a QUIC stream so it can be tested over an in-memory duplex.

use crate::message::Payload;
use anyhow::Result;

/// What the holder side of a handoff needs to do. Implemented by the
/// daemon (flush = ship the journal, release = lease CAS); the tests
/// supply a recording fake.
pub trait Handoff {
    /// Ship this partition's journal to S3 so the requester sees every
    /// committed record once it takes over. Returns the epoch the
    /// holder was operating under.
    fn flush_and_release(&mut self, part: &str) -> Result<Option<u64>>;
}

/// Decide how a holder answers a `LeaseRequest`.
///
/// `Some(reply)` is the message to send back. The holder declines
/// (`released: false`) when it does not hold the partition at all, so
/// the requester stops waiting on us and falls back to the S3 path
/// immediately instead of burning its deadline.
pub fn handle_request<H: Handoff>(holder: &mut H, part: &str, requester: u64) -> Result<Payload> {
    match holder.flush_and_release(part) {
        Ok(Some(epoch)) => {
            tracing::info!(part, requester, epoch, "handed the lease over on request");
            Ok(Payload::LeaseHandoff {
                part: part.to_string(),
                epoch,
                released: true,
                etag: None,
                head_seq: None,
            })
        }
        Ok(None) => Ok(Payload::LeaseHandoff {
            part: part.to_string(),
            epoch: 0,
            released: false,
            etag: None,
            head_seq: None,
        }),
        Err(e) => {
            // Declining on error is safe: the requester falls back to
            // waiting out the TTL, which is the pre-P2P behaviour.
            tracing::warn!(error = %e, part, requester, "lease handoff failed; declining");
            Ok(Payload::LeaseHandoff {
                part: part.to_string(),
                epoch: 0,
                released: false,
                etag: None,
                head_seq: None,
            })
        }
    }
}

/// What a requester should do with a reply. Deliberately narrow: the
/// only "yes" outcome is *try the CAS now*, never "you have the lease".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// The holder released; attempt the CAS claim immediately.
    ClaimNow,
    /// The holder kept it (busy, or does not hold it): keep polling S3.
    KeepWaiting,
}

pub fn interpret_reply(part: &str, reply: &Payload) -> RequestOutcome {
    match reply {
        Payload::LeaseHandoff {
            part: p,
            released: true,
            ..
        } if p == part => RequestOutcome::ClaimNow,
        _ => RequestOutcome::KeepWaiting,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Fake {
        /// Partitions this fake "holds", with their epoch.
        held: Vec<(String, u64)>,
        flushed: Vec<String>,
        fail: bool,
    }

    impl Handoff for Fake {
        fn flush_and_release(&mut self, part: &str) -> Result<Option<u64>> {
            if self.fail {
                anyhow::bail!("S3 unreachable");
            }
            self.flushed.push(part.to_string());
            match self.held.iter().position(|(p, _)| p == part) {
                Some(i) => Ok(Some(self.held.remove(i).1)),
                None => Ok(None),
            }
        }
    }

    #[test]
    fn holder_flushes_then_releases() {
        let mut f = Fake {
            held: vec![("p0".into(), 4)],
            ..Default::default()
        };
        let reply = handle_request(&mut f, "p0", 2).unwrap();
        assert_eq!(f.flushed, ["p0"], "must flush before handing over");
        assert_eq!(
            reply,
            Payload::LeaseHandoff {
                part: "p0".into(),
                epoch: 4,
                released: true,
                etag: None,
                head_seq: None,
            }
        );
        assert_eq!(interpret_reply("p0", &reply), RequestOutcome::ClaimNow);
    }

    /// Asking a node that does not hold the partition must not look like
    /// a successful handoff, or the requester would CAS pointlessly.
    #[test]
    fn non_holder_declines() {
        let mut f = Fake::default();
        let reply = handle_request(&mut f, "p0", 2).unwrap();
        assert_eq!(interpret_reply("p0", &reply), RequestOutcome::KeepWaiting);
    }

    /// A failed flush must decline rather than release: releasing with
    /// unshipped records would hand over a lease whose predecessor's
    /// writes are not yet visible.
    #[test]
    fn failed_flush_declines_instead_of_releasing() {
        let mut f = Fake {
            held: vec![("p0".into(), 9)],
            fail: true,
            ..Default::default()
        };
        let reply = handle_request(&mut f, "p0", 3).unwrap();
        assert_eq!(interpret_reply("p0", &reply), RequestOutcome::KeepWaiting);
        assert_eq!(f.held.len(), 1, "lease must still be held after a failure");
    }

    /// A reply about a different partition must never be taken as our
    /// handoff.
    #[test]
    fn reply_for_another_partition_is_ignored() {
        let reply = Payload::LeaseHandoff {
            part: "p1".into(),
            epoch: 2,
            released: true,
            etag: None,
            head_seq: None,
        };
        assert_eq!(interpret_reply("p0", &reply), RequestOutcome::KeepWaiting);
    }

    /// An unrelated message is never a handoff.
    #[test]
    fn unrelated_message_is_not_a_handoff() {
        assert_eq!(
            interpret_reply("p0", &Payload::Ping { node_id: 1 }),
            RequestOutcome::KeepWaiting
        );
    }
}
