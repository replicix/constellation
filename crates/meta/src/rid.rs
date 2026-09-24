//! Request identity for exactly-once forwarded mutations (plan 30 §M2,
//! the RIFL design applied to forwarding).
//!
//! Every `MutateOp` a FUSE call issues is assigned one `Rid` at the top
//! of `mutate_op_rebasable` (`crates/cli/src/fusefs.rs`) and keeps it
//! across every retry — the same holder, a redirected holder, or the
//! lease path — so the holder (and, after a takeover, whoever resolves
//! an in-doubt op against the log) can recognize a retried request and
//! answer from what already happened instead of executing it again.

use serde::{Deserialize, Serialize};

/// `(node, incarnation, seq)`. `incarnation` is this node's mount
/// counter — persisted in the `local` keyspace
/// (`store::KV_INCARNATION`) and bumped once, at every mount, before the
/// node serves any mutation. `seq` is a per-incarnation counter,
/// allocated once per client op and never reallocated for a retry of
/// that same op (a `SetManifest` rebase is a new op with a new rid, per
/// plan 30 §M2). The pair `(incarnation, seq)` is what actually keeps a
/// rid from ever being reused after a crash: `seq` itself resets to 0 on
/// every mount (the in-memory counter does not survive a crash), but the
/// incarnation bump means the *pair* never repeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Rid {
    pub node: u64,
    pub incarnation: u32,
    pub seq: u64,
}

impl Rid {
    /// Big-endian key for the `completed` keyspace: `node(8) ++
    /// incarnation(4) ++ seq(8)`. Big-endian throughout so a range scan
    /// over one node's rids (used by GC/retention) sorts by
    /// `(incarnation, seq)`, i.e. allocation order within that node.
    pub fn to_key(self) -> Vec<u8> {
        let mut k = Vec::with_capacity(20);
        k.extend_from_slice(&self.node.to_be_bytes());
        k.extend_from_slice(&self.incarnation.to_be_bytes());
        k.extend_from_slice(&self.seq.to_be_bytes());
        k
    }

    pub fn from_key(k: &[u8]) -> Option<Self> {
        if k.len() != 20 {
            return None;
        }
        Some(Rid {
            node: u64::from_be_bytes(k[0..8].try_into().ok()?),
            incarnation: u32::from_be_bytes(k[8..12].try_into().ok()?),
            seq: u64::from_be_bytes(k[12..20].try_into().ok()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrips() {
        let rid = Rid {
            node: 7,
            incarnation: 3,
            seq: 12345,
        };
        assert_eq!(Rid::from_key(&rid.to_key()), Some(rid));
    }

    #[test]
    fn key_orders_by_incarnation_then_seq() {
        let a = Rid {
            node: 1,
            incarnation: 1,
            seq: 9,
        };
        let b = Rid {
            node: 1,
            incarnation: 2,
            seq: 0,
        };
        assert!(a.to_key() < b.to_key());
    }
}
