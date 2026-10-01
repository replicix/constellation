//! [`InMemoryControl`]: a [`ControlClient`] fake with a `HashMap`-backed
//! tree, xattrs, quotas and snapshots — no S3, no `constellation-engine`.
//! Used by this crate's unit tests and, from K2 on, by `csi-sanity` (plan 37
//! §12: "a throwaway local `EngineProfile` ... so the sanity suite never
//! touches real S3").

use super::ControlClient;
use async_trait::async_trait;
use constellation_control::proto::types::{
    Ack, CloneParams, FileStat, FsCreateParams, FsCreated, FsUnlockParams, HandoffParams,
    HandoffReport, LeaveParams, MkdirParams, Pong, QuotaStatus, RenameParams, SetQuotaParams,
    SnapshotCreateParams, SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams,
    SnapshotListParams, SnapshotListing, SnapshotStatus, ViewInfo, ViewMountParams,
    ViewStatsParams, ViewStatsReport, ViewUnmountParams, XattrOp, XattrParams, XattrResult,
};
use constellation_control::proto::ControlError;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// One directory entry: just its extended attributes. There is no file
/// content in this fake — the CSI driver only ever creates/renames/xattrs
/// directories (`/volumes/<pv>`, `/.trash/<pv>-<ts>`), never writes file
/// data through the control protocol.
#[derive(Default, Clone)]
struct DirEntry {
    xattrs: BTreeMap<String, Vec<u8>>,
}

#[derive(Default)]
struct Mounted {
    /// Synthetic: real `PreopenedFd` views carry no mountpoint in
    /// `ViewMountParams` (K0 gap 3, `docs/plans/v1/wip/37-kubernetes-csi.md`
    /// "Gaps K0 found"), so this fake mints one at mount time and returns it
    /// in `ViewInfo.mountpoint` so `view_unmount` has something real to key
    /// on — a real engine pod's own matching is the open gap K3a inherits,
    /// not something this fake should silently paper over by matching on
    /// the wrong field.
    mountpoint: String,
}

struct State {
    /// Normalized path ("/", "/volumes/pvc-1") -> entry. The root always
    /// exists.
    tree: BTreeMap<String, DirEntry>,
    /// `(bucket, prefix)` -> the filesystem uuid `fs.create` minted for it.
    filesystems: BTreeMap<(String, String), String>,
    quota: QuotaStatus,
    snapshots: Vec<SnapshotStatus>,
    views: BTreeMap<u64, Mounted>,
}

/// An in-memory, in-process [`ControlClient`]: exercises the real control
/// shapes (idempotent `fs.create`, xattr-carried volume records, trash-style
/// renames, held snapshots) without a daemon, S3, or the engine. Not
/// durable, not concurrent-safe beyond its own `Mutex` (which is fine: a
/// single-process fake).
pub struct InMemoryControl {
    state: Mutex<State>,
    next_view_id: AtomicU64,
    next_fs_uuid: AtomicU64,
    /// A kill switch for `node_ping`, so callers (the Identity `Probe` RPC's
    /// unit tests, in particular) can exercise "the engine pod is dead"
    /// without a second `ControlClient` impl. Every other method stays
    /// healthy when this is set: only `node.ping` models a pod that stopped
    /// answering.
    unreachable: std::sync::atomic::AtomicBool,
}

impl Default for InMemoryControl {
    fn default() -> InMemoryControl {
        let mut tree = BTreeMap::new();
        tree.insert("/".to_string(), DirEntry::default());
        InMemoryControl {
            state: Mutex::new(State {
                tree,
                filesystems: BTreeMap::new(),
                quota: QuotaStatus::default(),
                snapshots: Vec::new(),
                views: BTreeMap::new(),
            }),
            next_view_id: AtomicU64::new(1),
            next_fs_uuid: AtomicU64::new(1),
            unreachable: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl InMemoryControl {
    /// From the next `node_ping` on, answer as if the engine pod were
    /// unreachable instead of healthy.
    pub fn mark_unreachable(&self) {
        self.unreachable.store(true, Ordering::SeqCst);
    }
}

/// `a/b/` -> `/a/b`; `a/b` -> `/a/b`; `` or `/` -> `/`.
fn normalize(path: &str) -> String {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn parent_of(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let slash = path.rfind('/').expect("normalized paths start with /");
    Some(if slash == 0 {
        "/".to_string()
    } else {
        path[..slash].to_string()
    })
}

impl InMemoryControl {
    /// Every entry whose path is `prefix` or lives under it, for a recursive
    /// rename/clone. `prefix` is always normalized (§[`normalize`]): `"/"` is
    /// the whole tree, since every path lives under the root; any other
    /// prefix never ends in `/`, so `"{prefix}/"` is an unambiguous
    /// separator between it and its children.
    fn subtree_of(tree: &BTreeMap<String, DirEntry>, prefix: &str) -> Vec<String> {
        if prefix == "/" {
            return tree.keys().cloned().collect();
        }
        let under = format!("{prefix}/");
        tree.keys()
            .filter(|p| *p == prefix || p.starts_with(&under))
            .cloned()
            .collect()
    }

    /// Rewrite `p` (a path returned by [`Self::subtree_of`] for `from`) as
    /// if its `from` subtree were rooted at `to` instead. `from == "/"`
    /// needs its own case: every path already starts with `/`, so slicing
    /// off `from`'s length (1) would eat the separator rather than just the
    /// prefix (e.g. `/volumes` would become `to` + `volumes`, not
    /// `to/volumes`).
    fn remap(from: &str, to: &str, p: &str) -> String {
        if from == "/" {
            normalize(&format!("{to}{p}"))
        } else {
            normalize(&format!("{to}{}", &p[from.len()..]))
        }
    }
}

#[async_trait]
impl ControlClient for InMemoryControl {
    async fn fs_create(&self, params: FsCreateParams) -> Result<FsCreated, ControlError> {
        let mut state = self.state.lock().unwrap();
        let key = (params.bucket.clone(), params.prefix.clone());
        if let Some(uuid) = state.filesystems.get(&key) {
            return Ok(FsCreated {
                uuid: uuid.clone(),
                created: false,
            });
        }
        let n = self.next_fs_uuid.fetch_add(1, Ordering::SeqCst);
        let uuid = format!("fake-fs-{n:08x}");
        state.filesystems.insert(key, uuid.clone());
        Ok(FsCreated {
            uuid,
            created: true,
        })
    }

    async fn fs_unlock(&self, _params: FsUnlockParams) -> Result<Ack, ControlError> {
        Ok(Ack::new("unlocked"))
    }

    async fn browse_mkdir(&self, params: MkdirParams) -> Result<FileStat, ControlError> {
        let path = normalize(&params.path);
        let mut state = self.state.lock().unwrap();
        if !params.parents {
            let parent = parent_of(&path).unwrap_or_else(|| "/".to_string());
            if !state.tree.contains_key(&parent) {
                return Err(ControlError::not_found(format!(
                    "{parent} does not exist (no --parents)"
                )));
            }
        } else {
            // Create every missing ancestor, root to leaf.
            let mut built = String::new();
            for part in path.trim_matches('/').split('/').filter(|p| !p.is_empty()) {
                built.push('/');
                built.push_str(part);
                state.tree.entry(built.clone()).or_default();
            }
        }
        state.tree.entry(path.clone()).or_default();
        Ok(FileStat {
            path,
            kind: "directory".to_string(),
            ..Default::default()
        })
    }

    async fn browse_xattr(&self, params: XattrParams) -> Result<XattrResult, ControlError> {
        let path = normalize(&params.path);
        let mut state = self.state.lock().unwrap();
        let entry = state
            .tree
            .get_mut(&path)
            .ok_or_else(|| ControlError::not_found(format!("{path} does not exist")))?;
        Ok(match params.op {
            XattrOp::Get { name } => XattrResult {
                value: entry.xattrs.get(&name).cloned().map(Into::into),
                names: Vec::new(),
            },
            XattrOp::List => XattrResult {
                value: None,
                names: entry.xattrs.keys().cloned().collect(),
            },
            XattrOp::Set { name, value } => {
                entry.xattrs.insert(name, value.0.to_vec());
                XattrResult::default()
            }
            XattrOp::Remove { name } => {
                entry.xattrs.remove(&name);
                XattrResult::default()
            }
        })
    }

    async fn browse_rename(&self, params: RenameParams) -> Result<Ack, ControlError> {
        let from = normalize(&params.from);
        let to = normalize(&params.to);
        if from == "/" {
            // The root always exists (invariant documented on `State::tree`);
            // renaming it away would violate that for every path still under
            // it, since nothing would re-create "/" afterwards.
            return Err(ControlError::invalid("cannot rename the root"));
        }
        let mut state = self.state.lock().unwrap();
        if !state.tree.contains_key(&from) {
            return Err(ControlError::not_found(format!("{from} does not exist")));
        }
        if state.tree.contains_key(&to) && !params.overwrite {
            return Err(ControlError::from(constellation_types::Code::Exists));
        }
        let moved: Vec<(String, DirEntry)> = InMemoryControl::subtree_of(&state.tree, &from)
            .into_iter()
            .map(|p| {
                let entry = state.tree.remove(&p).expect("just listed");
                (InMemoryControl::remap(&from, &to, &p), entry)
            })
            .collect();
        for (p, entry) in moved {
            state.tree.insert(p, entry);
        }
        Ok(Ack::new(format!("{from} -> {to}")))
    }

    async fn quota_get(&self) -> Result<QuotaStatus, ControlError> {
        Ok(self.state.lock().unwrap().quota.clone())
    }

    async fn quota_set(&self, params: SetQuotaParams) -> Result<QuotaStatus, ControlError> {
        let mut state = self.state.lock().unwrap();
        state.quota.max_bytes = params.max_bytes;
        Ok(state.quota.clone())
    }

    async fn snapshot_create(
        &self,
        params: SnapshotCreateParams,
    ) -> Result<SnapshotCreated, ControlError> {
        let (held, owner) = params.hold_request().map_err(ControlError::invalid)?;
        let id = format!("snap-{}", self.next_fs_uuid.fetch_add(1, Ordering::SeqCst));
        let snapshot = SnapshotStatus {
            id: id.clone(),
            path: params.selector.clone(),
            name: id,
            held,
            held_by: owner.map(str::to_string),
            origin: "manual".to_string(),
            ..Default::default()
        };
        self.state.lock().unwrap().snapshots.push(snapshot.clone());
        Ok(SnapshotCreated {
            detail: format!("snapshot of {} created", params.selector),
            snapshot,
        })
    }

    async fn snapshot_delete(&self, params: SnapshotDeleteParams) -> Result<Ack, ControlError> {
        let mut state = self.state.lock().unwrap();
        let Some(idx) = state
            .snapshots
            .iter()
            .position(|s| s.id == params.selector || s.path == params.selector)
        else {
            return Ok(Ack::new("already gone"));
        };
        if state.snapshots[idx].held && !params.force {
            return Err(ControlError::new(
                constellation_control::proto::ErrorKind::Conflict,
                format!("{} is held", params.selector),
            ));
        }
        state.snapshots.remove(idx);
        Ok(Ack::new("deleted"))
    }

    async fn snapshot_list(
        &self,
        params: SnapshotListParams,
    ) -> Result<SnapshotListing, ControlError> {
        let state = self.state.lock().unwrap();
        let snapshots = state
            .snapshots
            .iter()
            .filter(|s| params.path.as_deref().is_none_or(|p| s.path == p))
            .cloned()
            .collect();
        Ok(SnapshotListing { snapshots })
    }

    async fn snapshot_hold(
        &self,
        params: SnapshotHoldParams,
    ) -> Result<SnapshotHeld, ControlError> {
        let mut state = self.state.lock().unwrap();
        let snapshot = state
            .snapshots
            .iter_mut()
            .find(|s| s.id == params.id || s.path == params.id)
            .ok_or_else(|| ControlError::not_found(format!("no snapshot {}", params.id)))?;
        if snapshot.held
            && !params.force
            && snapshot.held_by.is_some()
            && snapshot.held_by != params.by
        {
            return Err(ControlError::denied(format!(
                "held by {}",
                snapshot.held_by.as_deref().unwrap_or("?")
            )));
        }
        snapshot.held = params.held;
        snapshot.held_by = params.by;
        Ok(SnapshotHeld {
            detail: format!("held: {}", snapshot.held),
            snapshot: snapshot.clone(),
        })
    }

    async fn clone_create(&self, params: CloneParams) -> Result<Ack, ControlError> {
        let from = normalize(&params.selector);
        let to = normalize(&params.destination);
        let mut state = self.state.lock().unwrap();
        if !state.tree.contains_key(&from) {
            return Err(ControlError::not_found(format!("{from} does not exist")));
        }
        let copies: Vec<(String, DirEntry)> = InMemoryControl::subtree_of(&state.tree, &from)
            .into_iter()
            .map(|p| {
                let entry = state.tree.get(&p).expect("just listed").clone();
                (InMemoryControl::remap(&from, &to, &p), entry)
            })
            .collect();
        for (p, entry) in copies {
            state.tree.insert(p, entry);
        }
        Ok(Ack::new(format!("{from} cloned to {to}")))
    }

    async fn view_mount(&self, params: ViewMountParams) -> Result<ViewInfo, ControlError> {
        let id = self.next_view_id.fetch_add(1, Ordering::SeqCst);
        let mountpoint = format!("/fake-mounts/{id}");
        self.state.lock().unwrap().views.insert(
            id,
            Mounted {
                mountpoint: mountpoint.clone(),
            },
        );
        Ok(ViewInfo {
            id,
            subtree: params.subtree,
            mountpoint,
            mounted_ms_ago: 0,
            labels: params.labels,
            qos: params.qos,
            confine_links: params.confine_links,
        })
    }

    async fn view_unmount(&self, params: ViewUnmountParams) -> Result<Ack, ControlError> {
        let mut state = self.state.lock().unwrap();
        let mountpoint = params.mountpoint.display().to_string();
        let Some(id) = state
            .views
            .iter()
            .find(|(_, v)| v.mountpoint == mountpoint)
            .map(|(id, _)| *id)
        else {
            return Err(ControlError::not_found(format!(
                "no view mounted at {mountpoint}"
            )));
        };
        state.views.remove(&id);
        Ok(Ack::new("unmounted"))
    }

    async fn view_stats(&self, params: ViewStatsParams) -> Result<ViewStatsReport, ControlError> {
        let state = self.state.lock().unwrap();
        let id = params
            .id
            .or_else(|| state.views.keys().next().copied())
            .ok_or_else(|| ControlError::not_found("no mounted view"))?;
        if !state.views.contains_key(&id) {
            return Err(ControlError::not_found(format!("no view {id}")));
        }
        Ok(ViewStatsReport {
            id,
            block_size: 4096,
            total_bytes: state.quota.max_bytes.unwrap_or(u64::MAX),
            used_bytes: state.quota.used_bytes,
            available_bytes: state
                .quota
                .max_bytes
                .map(|m| m.saturating_sub(state.quota.used_bytes))
                .unwrap_or(u64::MAX),
            inodes_total: u64::MAX,
            inodes_used: state.tree.len() as u64,
            rsize: state.quota.used_bytes,
            rcount: state.tree.len() as u64,
        })
    }

    async fn node_ping(&self) -> Result<Pong, ControlError> {
        if self.unreachable.load(Ordering::SeqCst) {
            return Err(ControlError::unavailable("engine pod is unreachable"));
        }
        Ok(Pong {})
    }

    async fn node_handoff(&self, params: HandoffParams) -> Result<HandoffReport, ControlError> {
        Ok(HandoffReport {
            detail: "fake handoff".to_string(),
            views: params
                .views
                .into_iter()
                .map(|id| constellation_control::proto::types::HandedOffView {
                    id,
                    mountpoint: String::new(),
                    handles: 0,
                })
                .collect(),
        })
    }

    async fn node_leave(&self, _params: LeaveParams) -> Result<Ack, ControlError> {
        Ok(Ack::new("left"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_control::proto::ErrorKind;

    #[tokio::test]
    async fn fs_create_is_idempotent_by_bucket_and_prefix() {
        let c = InMemoryControl::default();
        let a = c
            .fs_create(FsCreateParams {
                bucket: "b".into(),
                prefix: "p".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(a.created);
        let b = c
            .fs_create(FsCreateParams {
                bucket: "b".into(),
                prefix: "p".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!b.created);
        assert_eq!(a.uuid, b.uuid);

        let other = c
            .fs_create(FsCreateParams {
                bucket: "b".into(),
                prefix: "q".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_ne!(a.uuid, other.uuid);
    }

    #[tokio::test]
    async fn mkdir_xattr_and_rename_round_trip() {
        let c = InMemoryControl::default();

        // mkdir without --parents refuses a missing parent.
        let err = c
            .browse_mkdir(MkdirParams {
                path: "/volumes/pv1".into(),
                mode: None,
                parents: false,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::NotFound);

        c.browse_mkdir(MkdirParams {
            path: "/volumes".into(),
            mode: None,
            parents: false,
        })
        .await
        .unwrap();
        c.browse_mkdir(MkdirParams {
            path: "/volumes/pv1".into(),
            mode: None,
            parents: false,
        })
        .await
        .unwrap();

        c.browse_xattr(XattrParams {
            path: "/volumes/pv1".into(),
            op: XattrOp::Set {
                name: "user.constellation.csi.pv".into(),
                value: b"pv1".to_vec().into(),
            },
        })
        .await
        .unwrap();

        let got = c
            .browse_xattr(XattrParams {
                path: "/volumes/pv1".into(),
                op: XattrOp::Get {
                    name: "user.constellation.csi.pv".into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(got.value.unwrap().0.as_ref(), b"pv1");

        c.browse_rename(RenameParams {
            from: "/volumes/pv1".into(),
            to: "/.trash/pv1-1".into(),
            overwrite: false,
        })
        .await
        .unwrap();

        let gone = c
            .browse_xattr(XattrParams {
                path: "/volumes/pv1".into(),
                op: XattrOp::List,
            })
            .await
            .unwrap_err();
        assert_eq!(gone.kind, ErrorKind::NotFound);

        let moved = c
            .browse_xattr(XattrParams {
                path: "/.trash/pv1-1".into(),
                op: XattrOp::Get {
                    name: "user.constellation.csi.pv".into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(moved.value.unwrap().0.as_ref(), b"pv1");
    }

    #[tokio::test]
    async fn quota_set_then_get() {
        let c = InMemoryControl::default();
        assert_eq!(c.quota_get().await.unwrap().max_bytes, None);
        let status = c
            .quota_set(SetQuotaParams {
                max_bytes: Some(1024),
            })
            .await
            .unwrap();
        assert_eq!(status.max_bytes, Some(1024));
        assert_eq!(c.quota_get().await.unwrap().max_bytes, Some(1024));
    }

    #[tokio::test]
    async fn snapshot_create_hold_delete() {
        let c = InMemoryControl::default();
        c.browse_mkdir(MkdirParams {
            path: "/volumes/pv1".into(),
            mode: None,
            parents: true,
        })
        .await
        .unwrap();

        let created = c
            .snapshot_create(SnapshotCreateParams {
                selector: "/volumes/pv1".into(),
                held_by: Some("csi:content-1".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(created.snapshot.held);
        assert_eq!(created.snapshot.held_by.as_deref(), Some("csi:content-1"));

        // A held snapshot refuses delete without force.
        let refused = c
            .snapshot_delete(SnapshotDeleteParams {
                selector: created.snapshot.id.clone(),
                force: false,
            })
            .await
            .unwrap_err();
        assert_eq!(refused.kind, ErrorKind::Conflict);

        // Release the hold, then delete cleanly.
        c.snapshot_hold(SnapshotHoldParams {
            id: created.snapshot.id.clone(),
            held: false,
            by: Some("csi:content-1".into()),
            force: false,
        })
        .await
        .unwrap();
        c.snapshot_delete(SnapshotDeleteParams {
            selector: created.snapshot.id.clone(),
            force: false,
        })
        .await
        .unwrap();

        let listing = c
            .snapshot_list(SnapshotListParams::default())
            .await
            .unwrap();
        assert!(listing.snapshots.is_empty());

        // Deleting an already-gone snapshot is OK (idempotent).
        c.snapshot_delete(SnapshotDeleteParams {
            selector: created.snapshot.id,
            force: false,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn clone_copies_the_subtree() {
        let c = InMemoryControl::default();
        c.browse_mkdir(MkdirParams {
            path: "/volumes/source".into(),
            mode: None,
            parents: true,
        })
        .await
        .unwrap();
        c.browse_xattr(XattrParams {
            path: "/volumes/source".into(),
            op: XattrOp::Set {
                name: "user.constellation.csi.pv".into(),
                value: b"source".to_vec().into(),
            },
        })
        .await
        .unwrap();

        c.clone_create(CloneParams {
            selector: "/volumes/source".into(),
            destination: "/volumes/clone".into(),
        })
        .await
        .unwrap();

        let got = c
            .browse_xattr(XattrParams {
                path: "/volumes/clone".into(),
                op: XattrOp::Get {
                    name: "user.constellation.csi.pv".into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(got.value.unwrap().0.as_ref(), b"source");
    }

    /// `selector: "/"` is how every `layout: dedicated` snapshot/clone names
    /// its source (plan 37 §3 decision 8, §5): the whole tree, not a literal
    /// path match of just the root entry. Every other path must land one
    /// level under the destination, with the separator intact, and the
    /// root's own entry must become the destination itself.
    #[tokio::test]
    async fn clone_from_root_remaps_the_whole_tree() {
        let c = InMemoryControl::default();
        for p in ["/volumes/source", "/volumes/source/sub", "/other"] {
            c.browse_mkdir(MkdirParams {
                path: p.into(),
                mode: None,
                parents: true,
            })
            .await
            .unwrap();
        }
        c.browse_xattr(XattrParams {
            path: "/".into(),
            op: XattrOp::Set {
                name: "user.constellation.csi.root".into(),
                value: b"yes".to_vec().into(),
            },
        })
        .await
        .unwrap();

        c.clone_create(CloneParams {
            selector: "/".into(),
            destination: "/volumes/clone".into(),
        })
        .await
        .unwrap();

        // The root's own entry becomes the destination (not
        // "/volumes/clonevolumes" or similar path corruption).
        let root_clone = c
            .browse_xattr(XattrParams {
                path: "/volumes/clone".into(),
                op: XattrOp::Get {
                    name: "user.constellation.csi.root".into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(root_clone.value.unwrap().0.as_ref(), b"yes");
        // Every original path survives untouched (clone, not move) and the
        // destination mirrors the source tree one level down, separator
        // intact.
        for p in [
            "/volumes/source",
            "/volumes/clone/volumes/source",
            "/volumes/clone/volumes/source/sub",
            "/volumes/clone/other",
        ] {
            c.browse_xattr(XattrParams {
                path: p.into(),
                op: XattrOp::List,
            })
            .await
            .unwrap_or_else(|e| panic!("{p} should exist after cloning /: {e}"));
        }
        // The root invariant holds: "/" itself is untouched by a clone.
        c.browse_xattr(XattrParams {
            path: "/".into(),
            op: XattrOp::List,
        })
        .await
        .unwrap();
    }

    /// Renaming the root would leave nothing to satisfy `State::tree`'s "the
    /// root always exists" invariant (`subtree_of`/`remap` can compute the
    /// move, but nothing would re-create "/" afterwards) — refused outright.
    #[tokio::test]
    async fn browse_rename_refuses_the_root() {
        let c = InMemoryControl::default();
        let err = c
            .browse_rename(RenameParams {
                from: "/".into(),
                to: "/volumes/escaped".into(),
                overwrite: false,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Invalid);
        // The root is still there.
        c.browse_mkdir(MkdirParams {
            path: "/still-here".into(),
            mode: None,
            parents: false,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn view_mount_unmount_and_stats() {
        let c = InMemoryControl::default();
        let view = c
            .view_mount(ViewMountParams {
                subtree: "/volumes/pv1".into(),
                source: constellation_control::proto::types::MountSource::PreopenedFd,
                labels: Default::default(),
                qos: Default::default(),
                confine_links: false,
            })
            .await
            .unwrap();
        assert_eq!(view.subtree, "/volumes/pv1");

        let stats = c
            .view_stats(ViewStatsParams {
                id: Some(view.id),
                mountpoint: None,
            })
            .await
            .unwrap();
        assert_eq!(stats.id, view.id);
        assert!(!view.mountpoint.is_empty());

        // Unmounting the wrong mountpoint (e.g. the subtree, not where it
        // was actually mounted) is a miss, not a silent no-op.
        let miss = c
            .view_unmount(ViewUnmountParams {
                mountpoint: "/volumes/pv1".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(miss.kind, ErrorKind::NotFound);

        c.view_unmount(ViewUnmountParams {
            mountpoint: view.mountpoint.clone().into(),
        })
        .await
        .unwrap();

        let missing = c
            .view_stats(ViewStatsParams {
                id: Some(view.id),
                mountpoint: None,
            })
            .await
            .unwrap_err();
        assert_eq!(missing.kind, ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn node_ping_and_leave() {
        let c = InMemoryControl::default();
        c.node_ping().await.unwrap();
        c.node_leave(LeaveParams::default()).await.unwrap();
    }

    #[tokio::test]
    async fn mark_unreachable_fails_node_ping_only() {
        let c = InMemoryControl::default();
        c.mark_unreachable();
        let err = c.node_ping().await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unavailable);
        // Everything else still works: the kill switch models a dead
        // engine pod, not a broken fake.
        c.quota_get().await.unwrap();
    }
}
