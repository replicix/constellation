//! Metadata log records (DESIGN.md §4): the versioned, append-only op
//! registry. Serialized as JSON within zstd-batched segments.

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub xattrs: Vec<(String, Vec<u8>)>,
}

/// One metadata operation. Field names are stable format surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// Split a directory off as its own partition. Carried on the
    /// *parent* partition's stream; the child stream starts empty at
    /// seq 1 after this record is durable (DESIGN.md §4 "Partitions").
    PartSplit {
        part: String,
        at_ino: Ino,
        new_part: String,
        time_ns: i64,
    },
    /// Absorb a child partition back into `into_part`. Carried on the
    /// surviving (parent) stream; the child's stream is then sealed.
    PartMerge {
        part: String,
        into_part: String,
        time_ns: i64,
    },
    /// Source half of a cross-partition rename (linked two-record
    /// commit). Applied only when the matching [`RenameXpartDst`] is
    /// also present; otherwise parked, and aborted if the dst never
    /// appears (see `replay` module docs).
    RenameXpartSrc {
        txid: u64,
        part: String,
        from_parent: Ino,
        name: String,
        ino: Ino,
        time_ns: i64,
    },
    /// Destination half of a cross-partition rename.
    RenameXpartDst {
        txid: u64,
        part: String,
        to_parent: Ino,
        new_name: String,
        ino: Ino,
        time_ns: i64,
    },
    /// Void an orphan [`RenameXpartSrc`] whose dst half never landed.
    /// Only the current holder of the src partition may append this.
    RenameXpartAbort { txid: u64 },
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip() {
        let r = LogRecord::Create {
            parent: 1,
            name: "hello.txt".into(),
            ino: 42,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            time_ns: 123,
        };
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("\"op\":\"create\""));
        assert_eq!(serde_json::from_str::<LogRecord>(&s).unwrap(), r);
    }
}
