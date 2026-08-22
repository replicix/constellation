//! Metadata log records (DESIGN.md §4): the versioned, append-only op
//! registry. Serialized as JSON within zstd-batched segments.

use constellation_fs_core::Ino;
use serde::{Deserialize, Serialize};

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
        /// Encoded `fs_core::Manifest` bytes.
        manifest: Vec<u8>,
        size: u64,
        time_ns: i64,
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
