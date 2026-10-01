//! The `volume_id` codec (plan 37 settled decision 7): every Controller and
//! Node RPC finds the right filesystem and subtree from `req.volume_id`
//! alone, with no registry, CRD or controller-side table to consult.
//!
//! Three shapes, told apart by the first `/`-separated segment:
//!
//! | layout | `volume_id` | filesystem | subtree |
//! |---|---|---|---|
//! | pool | `v1/pool/<shard>/<fs-uuid>/volumes/<name>` | `<fs-uuid>` (the shard's own filesystem) | `/volumes/<name>` |
//! | dedicated | `v1/dedicated/<fs-uuid>/` | `<fs-uuid>` | `/` |
//! | static | `<fs-uuid>/<any/existing/path>` | `<fs-uuid>` | `/<path>` |
//!
//! The tail after the `v1/<layout>/[<shard>/]` header is exactly settled
//! decision 7's `<fs-uuid>/<path-within-pool>`, so a human reading a PV's
//! `volumeHandle` sees the pool filesystem and the path to mount by hand
//! (settled decision 18). The header adds what the plan's bare form could
//! not carry: the **version** (a later codec can change the tail without
//! guessing which ids are old), the **layout** (`DeleteVolume` trashes a
//! pool subtree but would drop a whole dedicated filesystem) and the
//! **shard index** (the `shard` view label and the same-shard clone check,
//! settled decision 8, need it; the uuid alone only says *which*
//! filesystem, not which of the class's N shards it is).
//!
//! Unversioned ids are statically provisioned (settled decision 17):
//! whatever an operator wrote as the PV's `volumeHandle`. A first segment of
//! the form `v<digits>` is reserved for this codec, so a static handle can
//! never be mistaken for a versioned one — and an unknown version is
//! refused rather than read as a static path.

use std::fmt;

/// The directory every pool volume lives under (settled decision 7).
pub const VOLUMES_DIR: &str = "/volumes";
/// Where `DeleteVolume` moves a pool volume (settled decision 19).
pub const TRASH_DIR: &str = "/.trash";

const VERSION: &str = "v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeId {
    /// A dynamically provisioned pool volume: `/volumes/<name>` inside
    /// shard `shard`'s filesystem.
    Pool {
        shard: u32,
        fs_uuid: String,
        name: String,
    },
    /// A whole filesystem per PV (`layout: dedicated`).
    Dedicated { fs_uuid: String },
    /// A statically provisioned handle: any path inside any filesystem.
    /// `path` is absolute and normalized (`/` for the root).
    Static { fs_uuid: String, path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseError {}

fn err(id: &str, why: impl fmt::Display) -> ParseError {
    ParseError(format!("volume_id {id:?}: {why}"))
}

/// A filesystem uuid as it appears in an id: non-empty, and only the
/// characters a uuid (or this crate's fake's `fake-fs-…`) is made of, so it
/// can never smuggle a separator or a path component.
fn valid_uuid(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// A volume name is one path component: `CreateVolume`'s `req.name`
/// becomes `/volumes/<name>` and `/.trash/<name>-<ts>`, so it must not be
/// empty, `.`/`..`, contain `/` or NUL, or be too long to leave room for
/// the trash suffix inside a 255-byte name.
pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name is empty".into());
    }
    if name == "." || name == ".." {
        return Err(format!("name {name:?} is not a valid directory name"));
    }
    if name.contains('/') || name.contains('\0') {
        return Err(format!("name {name:?} contains '/' or NUL"));
    }
    // `-<unix ms>` is at most 21 bytes.
    if name.len() > 255 - 21 {
        return Err(format!("name is {} bytes, the limit is 234", name.len()));
    }
    Ok(())
}

impl VolumeId {
    pub fn parse(id: &str) -> Result<VolumeId, ParseError> {
        if id.is_empty() {
            return Err(err(id, "empty"));
        }
        let (first, rest) = id.split_once('/').ok_or_else(|| {
            err(
                id,
                "expected <fs-uuid>/<path> or v1/<layout>/..., found no '/'",
            )
        })?;
        let versioned = first.len() > 1
            && first.starts_with('v')
            && first[1..].bytes().all(|b| b.is_ascii_digit());
        if !versioned {
            if !valid_uuid(first) {
                return Err(err(id, format!("{first:?} is not a filesystem uuid")));
            }
            let path = normalize_static(id, rest)?;
            return Ok(VolumeId::Static {
                fs_uuid: first.to_string(),
                path,
            });
        }
        if first != VERSION {
            return Err(err(
                id,
                format!("unsupported codec version {first:?} (this driver reads {VERSION})"),
            ));
        }
        let (layout, rest) = rest
            .split_once('/')
            .ok_or_else(|| err(id, "missing layout"))?;
        match layout {
            "pool" => {
                let mut parts = rest.splitn(4, '/');
                let shard = parts.next().unwrap_or_default();
                let fs_uuid = parts.next().unwrap_or_default();
                let volumes = parts.next().unwrap_or_default();
                let name = parts.next().unwrap_or_default();
                let shard: u32 = shard
                    .parse()
                    .ok()
                    // Canonical decimal only, so one volume has one id.
                    .filter(|_| shard.bytes().all(|b| b.is_ascii_digit()))
                    .filter(|_| shard == "0" || !shard.starts_with('0'))
                    .ok_or_else(|| err(id, format!("shard {shard:?} is not a number")))?;
                if !valid_uuid(fs_uuid) {
                    return Err(err(id, format!("{fs_uuid:?} is not a filesystem uuid")));
                }
                if volumes != VOLUMES_DIR.trim_start_matches('/') {
                    return Err(err(id, "a pool volume lives under volumes/"));
                }
                validate_name(name).map_err(|why| err(id, why))?;
                Ok(VolumeId::Pool {
                    shard,
                    fs_uuid: fs_uuid.to_string(),
                    name: name.to_string(),
                })
            }
            "dedicated" => {
                let fs_uuid = rest
                    .strip_suffix('/')
                    .ok_or_else(|| err(id, "a dedicated id ends in '/' (the root)"))?;
                if !valid_uuid(fs_uuid) {
                    return Err(err(id, format!("{fs_uuid:?} is not a filesystem uuid")));
                }
                Ok(VolumeId::Dedicated {
                    fs_uuid: fs_uuid.to_string(),
                })
            }
            other => Err(err(id, format!("unknown layout {other:?}"))),
        }
    }

    pub fn fs_uuid(&self) -> &str {
        match self {
            VolumeId::Pool { fs_uuid, .. }
            | VolumeId::Dedicated { fs_uuid }
            | VolumeId::Static { fs_uuid, .. } => fs_uuid,
        }
    }

    /// The subtree this volume is, inside [`Self::fs_uuid`].
    pub fn subtree(&self) -> String {
        match self {
            VolumeId::Pool { name, .. } => format!("{VOLUMES_DIR}/{name}"),
            VolumeId::Dedicated { .. } => "/".to_string(),
            VolumeId::Static { path, .. } => path.clone(),
        }
    }
}

/// A static handle's path, normalized to an absolute path with no empty,
/// `.` or `..` components — a handle must not name anything above the
/// filesystem root, and two spellings of one path must not be two volumes.
fn normalize_static(id: &str, rest: &str) -> Result<String, ParseError> {
    let mut parts = Vec::new();
    for part in rest.split('/') {
        match part {
            "" => {}
            "." | ".." => return Err(err(id, format!("path component {part:?}"))),
            p if p.contains('\0') => return Err(err(id, "NUL in path")),
            p => parts.push(p),
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

impl fmt::Display for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VolumeId::Pool {
                shard,
                fs_uuid,
                name,
            } => write!(f, "{VERSION}/pool/{shard}/{fs_uuid}{VOLUMES_DIR}/{name}"),
            VolumeId::Dedicated { fs_uuid } => write!(f, "{VERSION}/dedicated/{fs_uuid}/"),
            VolumeId::Static { fs_uuid, path } => {
                write!(f, "{fs_uuid}/{}", path.trim_start_matches('/'))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "4f9c1e2a-0b1c-4d5e-8f90-1a2b3c4d5e6f";

    #[test]
    fn pool_round_trips_and_keeps_the_plan_shaped_tail() {
        let id = VolumeId::Pool {
            shard: 2,
            fs_uuid: UUID.into(),
            name: "pvc-1a2b".into(),
        };
        let s = id.to_string();
        assert_eq!(s, format!("v1/pool/2/{UUID}/volumes/pvc-1a2b"));
        // Settled decision 7's `<fs-uuid>/<path-within-pool>` survives as
        // the tail.
        assert!(s.ends_with(&format!("{UUID}/volumes/pvc-1a2b")));
        assert_eq!(VolumeId::parse(&s).unwrap(), id);
        assert_eq!(id.subtree(), "/volumes/pvc-1a2b");
        assert_eq!(id.fs_uuid(), UUID);
    }

    #[test]
    fn dedicated_round_trips() {
        let id = VolumeId::Dedicated {
            fs_uuid: UUID.into(),
        };
        let s = id.to_string();
        assert_eq!(s, format!("v1/dedicated/{UUID}/"));
        assert_eq!(VolumeId::parse(&s).unwrap(), id);
        assert_eq!(id.subtree(), "/");
    }

    #[test]
    fn unversioned_ids_are_static_handles() {
        let id = VolumeId::parse(&format!("{UUID}/datasets//imagenet/")).unwrap();
        assert_eq!(
            id,
            VolumeId::Static {
                fs_uuid: UUID.into(),
                path: "/datasets/imagenet".into()
            }
        );
        assert_eq!(id.to_string(), format!("{UUID}/datasets/imagenet"));
        let root = VolumeId::parse(&format!("{UUID}/")).unwrap();
        assert_eq!(root.subtree(), "/");
        // A static handle that happens to look like a pool path is still
        // static: only the `v1/` header makes an id ours.
        let lookalike = VolumeId::parse(&format!("{UUID}/volumes/pvc-x")).unwrap();
        assert!(matches!(lookalike, VolumeId::Static { .. }));
    }

    #[test]
    fn malformed_ids_are_refused() {
        for bad in [
            "",
            "reallyfakevolumeid",
            "v2/pool/0/abc/volumes/x",
            "v1/zfs/abc/",
            "v1/pool/x/abc/volumes/pv",
            "v1/pool/-1/abc/volumes/pv",
            "v1/pool/+1/abc/volumes/pv",
            "v1/pool/01/abc/volumes/pv",
            "v1/pool/0/abc/other/pv",
            "v1/pool/0/abc/volumes/",
            "v1/pool/0/abc/volumes/a/b",
            "v1/pool/0/abc/volumes/..",
            "v1/pool/0//volumes/pv",
            "v1/pool/0/a_b/volumes/pv",
            "v1/dedicated/abc",
            "v1/dedicated/abc/def/",
            "bad uuid/path",
            "abc/../escape",
            "abc/./x",
        ] {
            assert!(VolumeId::parse(bad).is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn names_are_one_path_component() {
        validate_name("pvc-1a2b3c").unwrap();
        validate_name(&"x".repeat(128)).unwrap();
        for bad in ["", ".", "..", "a/b", "a\0b"] {
            assert!(validate_name(bad).is_err(), "{bad:?}");
        }
        assert!(validate_name(&"x".repeat(235)).is_err());
    }
}
