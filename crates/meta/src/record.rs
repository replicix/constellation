//! Metadata log records (DESIGN.md §4): the versioned, append-only op
//! registry. Serialized as postcard in the local journal and in zstd
//! S3 log segments (same codec as P2P gossip envelopes).

use crate::rid::Rid;
use constellation_fs_core::Ino;
use serde::{Deserialize, Serialize};

/// One eagerly materialized clone inode.  Inode numbers are allocated by
/// the creator and carried in the single `clone` record so every replica
/// reconstructs exactly the same ordinary subtree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloneNode {
    pub parent: Ino,
    pub name: String,
    pub ino: Ino,
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub rdev: u64,
    pub target: Option<String>,
    pub manifest: Option<Vec<u8>>,
    #[serde(default)]
    pub xattrs: Vec<(String, Vec<u8>)>,
}

/// One metadata operation.
///
/// Externally tagged so the same `Serialize` impl works for postcard
/// (non-self-describing). Do not add `skip_serializing_if`: postcard
/// would then mis-align fields on decode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogRecord {
    Mkdir {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
        time_ns: i64,
    },
    Create {
        parent: Ino,
        name: String,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
        time_ns: i64,
    },
    Symlink {
        parent: Ino,
        name: String,
        ino: Ino,
        target: String,
        uid: u32,
        gid: u32,
        time_ns: i64,
    },
    Mknod {
        parent: Ino,
        name: String,
        ino: Ino,
        kind: u8,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u64,
        time_ns: i64,
    },
    Link {
        ino: Ino,
        parent: Ino,
        name: String,
        time_ns: i64,
    },
    Unlink {
        parent: Ino,
        name: String,
        time_ns: i64,
    },
    Rmdir {
        parent: Ino,
        name: String,
        time_ns: i64,
    },
    Rename {
        parent: Ino,
        name: String,
        new_parent: Ino,
        new_name: String,
        time_ns: i64,
    },
    Setattr {
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        #[serde(default)]
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
        time_ns: i64,
    },
    WriteManifest {
        ino: Ino,
        /// Manifest this edit was based on. Reintegration may append
        /// cleanly only when the shared winner still equals this value.
        base_manifest: Option<Vec<u8>>,
        /// Encoded `fs_core::Manifest` bytes.
        manifest: Vec<u8>,
        size: u64,
        time_ns: i64,
    },
    SetXattr {
        ino: Ino,
        name: String,
        value: Vec<u8>,
        time_ns: i64,
    },
    RemoveXattr {
        ino: Ino,
        name: String,
        time_ns: i64,
    },
    SnapCreate {
        id: String,
        path: String,
        name: String,
        root_hash: String,
        created_unix_ms: i64,
    },
    SnapDelete {
        id: String,
        path: String,
        name: String,
    },
    Clone {
        source_path: String,
        snapshot: String,
        root_hash: String,
        nodes: Vec<CloneNode>,
    },
    /// Cluster-wide logical byte cap. `None` clears the quota (unlimited).
    /// Journaled on `p0`; replay upserts `kv.quota_max_bytes`.
    SetQuota { max_logical_bytes: Option<u64> },
    /// A read-time access-time bump. Best-effort and order-free: replay
    /// merges with max(), so a duplicate, delayed, or out-of-order
    /// record can never make replicas disagree, and a dropped one only
    /// costs freshness. Deliberately *not* a `Setattr`: it must not
    /// touch ctime and must not participate in conflict detection (see
    /// `TouchSet::add` and `apply_one`).
    Atime {
        ino: Ino,
        /// The access time being claimed.
        atime_ns: i64,
        /// When the emitting node observed the read. Used to drop bumps
        /// that were already in flight when someone set atime
        /// explicitly (the `ctime_ns < time_ns` guard in replay).
        time_ns: i64,
    },
    /// Plan 30 §M2 (RIFL for forwarding): `rid` identifies the op whose
    /// own records this shipped alongside, in the same fjall
    /// transaction. Applying it inserts `rid -> position` into the
    /// node-local, unpublished `completed` keyspace on every replica
    /// that tails it — never the tree, never `dirty`. Touches no
    /// inode/dentry, so it never enters conflict detection (see
    /// `TouchSet::add`), exactly like `Atime`. Plan 30 §M3b: a
    /// transaction's records and its `Completed` always ship in the same
    /// segment (`Meta::whole_tx_prefix`).
    ///
    /// Appended last, like `Atime`: a peer too old to decode it would
    /// simply fail to parse this record, which the plan accepts (plan
    /// 30 explicitly waives wire/on-disk compatibility) but which this
    /// ordering makes moot in the one case that still matters — a
    /// mid-rollout mismatch is out of scope, not a live concern.
    Completed { rid: Rid },
    /// Plan 30 §M13: the holder refused the inbox-submitted op `rid` with
    /// `errno`. An inbox op has no reply to carry its refusal, so it rides
    /// the log like a completion; applying it inserts `rid -> refused(errno)`
    /// into `completed`, and every dedup site answers the rid with that
    /// errno from then on — a refusal is an outcome, never re-evaluated
    /// (see `docs/reference/features/forwarded-mutations.md`, "The inbox").
    /// Touches no inode/dentry (see `TouchSet::add`). Only inbox-executed
    /// ops produce this record; a P2P refusal still rides its reply.
    Refused { rid: Rid, errno: i32 },
    /// Plan 30 §M13: position `(n, i)` of requester `node`'s inbox batch
    /// under `epoch` has an outcome (executed, refused or deduplicated)
    /// in this transaction. Every replica keeps the highest such position
    /// per `(epoch, node)` in a node-local watermark that retention never
    /// prunes, so a takeover drain older than the `completed` retention
    /// window still skips what was already answered. Touches nothing.
    InboxAck {
        epoch: u64,
        node: u64,
        n: u64,
        i: u32,
    },
    /// Plan 30 §M11: `dir` and everything under it is sequenced by
    /// `node` under delegation generation `gen` from this record on. The
    /// replicated delegation table is the fold of these records; applying
    /// one updates the `0x30 | Delegation` row (`crate::delegation`), so a
    /// commit carries the table and a bootstrapped replica learns it.
    /// Touches no inode or dentry (see `TouchSet::add`).
    Delegate {
        dir: u64,
        node: u64,
        gen: u64,
        /// Plan 30 §M11 phase 2b: an offline designation (plans 03–05):
        /// never recalled by TTL or placement, only by `online`; a
        /// cross-subtree op touching it is refused (`EXDEV`).
        #[serde(default)]
        designated: bool,
    },
    /// Plan 30 §M11: generation `gen` of the delegation on `dir` ended.
    /// Every record of its stream that is not before this one in the log
    /// is void: a delegate that still holds such records rolls them back
    /// and replays them by rid, and a requester's observation of them is
    /// void (M6's rule for a tenure that ended without shipping).
    Recall { dir: u64, gen: u64 },
}

impl LogRecord {
    /// Encode for the local journal BLOB and for S3 segment payloads.
    pub fn to_postcard(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    /// Decode a journal / segment record.
    pub fn from_postcard(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postcard_roundtrip() {
        let r = LogRecord::Create {
            parent: 1,
            name: "hello.txt".into(),
            ino: 42,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            time_ns: 123,
        };
        let bytes = r.to_postcard().unwrap();
        assert_eq!(LogRecord::from_postcard(&bytes).unwrap(), r);
    }

    #[test]
    fn postcard_write_manifest_keeps_binary() {
        let manifest = b"CMF1\0\0@\0".to_vec();
        let r = LogRecord::WriteManifest {
            ino: 7,
            base_manifest: None,
            manifest: manifest.clone(),
            size: 99,
            time_ns: 1,
        };
        let bytes = r.to_postcard().unwrap();
        // Binary payload must appear verbatim, not as a JSON number list.
        assert!(bytes.windows(manifest.len()).any(|w| w == manifest));
        assert_eq!(LogRecord::from_postcard(&bytes).unwrap(), r);
    }
}
