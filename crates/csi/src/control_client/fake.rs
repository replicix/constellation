//! [`InMemoryControl`]: a [`ControlClient`] fake with a `HashMap`-backed
//! tree, xattrs, quotas and snapshots — no S3, no `constellation-engine`.
//! Used by this crate's unit tests and, from K2 on, by `csi-sanity` (plan 37
//! §12: "a throwaway local `EngineProfile` ... so the sanity suite never
//! touches real S3").

use super::{ControlClient, Engines, Handle, PoolRef, SubtreeQuotaParams};
use async_trait::async_trait;
use constellation_control::fd::OwnedFd;
use constellation_control::proto::types::{
    Ack, CloneParams, FileStat, FsCreateParams, FsCreated, FsInfo, FsListing, FsUnlockParams,
    HandedOffView, HandoffParams, HandoffPhase, HandoffReport, HandoffState, HandoffTarget,
    LeaveParams, MkdirParams, MountSource, Pong, QuotaStatus, RenameParams, SnapshotCreateParams,
    SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams, SnapshotListParams,
    SnapshotListing, SnapshotStatus, ViewInfo, ViewListParams, ViewListing, ViewMountParams,
    ViewStatsParams, ViewStatsReport, ViewUnmountParams, XattrOp, XattrParams, XattrResult,
};
use constellation_control::proto::ControlError;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One directory entry: its extended attributes and its own subtree quota
/// (plan 37 §5's `quota.set{subtree}`; the root's is the filesystem-wide
/// cap). There is no file content in this fake — the CSI driver only ever
/// creates/renames/xattrs directories (`/volumes/<pv>`, `/.trash/<pv>-<ts>`),
/// never writes file data through the control protocol — so `used_bytes`
/// is whatever a test planted with [`InMemoryControl::set_used_bytes`].
/// Keeping the quota on the entry makes it travel with a rename or clone,
/// as a real per-directory quota would.
#[derive(Default, Clone)]
struct DirEntry {
    xattrs: BTreeMap<String, Vec<u8>>,
    quota: Option<u64>,
    used_bytes: u64,
}

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
struct Mounted {
    /// What the view is known by, as the daemon names it: the `Path`
    /// mountpoint, a `PreopenedFd` view's sender-given mountpoint, or a
    /// minted `fd:<id>` for one sent without a name.
    mountpoint: String,
    subtree: String,
    labels: BTreeMap<String, String>,
}

/// The filesystem registry `fs.create` writes: shared by every
/// [`InMemoryControl`] an [`InMemoryEngines`] hands out, the way every
/// engine pod reads one registry, so a uuid minted through one client is
/// known to all of them.
#[derive(Default)]
struct Registry {
    /// `(bucket, prefix)` -> the filesystem uuid `fs.create` minted for it.
    filesystems: BTreeMap<(String, String), String>,
    next_fs_uuid: u64,
}

/// A snapshot and the subtree it froze (paths relative to its root, `/`
/// being the root itself).
struct FrozenSnapshot {
    status: SnapshotStatus,
    frozen: Vec<(String, DirEntry)>,
}

struct State {
    /// Normalized path ("/", "/volumes/pvc-1") -> entry. The root always
    /// exists.
    tree: BTreeMap<String, DirEntry>,
    snapshots: Vec<FrozenSnapshot>,
    views: BTreeMap<u64, Mounted>,
}

/// An in-memory, in-process [`ControlClient`]: exercises the real control
/// shapes (idempotent `fs.create`, xattr-carried volume records, trash-style
/// renames, held snapshots) without a daemon, S3, or the engine. Not
/// durable, not concurrent-safe beyond its own `Mutex` (which is fine: a
/// single-process fake). One instance is one filesystem's tree; the
/// registry behind `fs.create` may be shared ([`InMemoryEngines`]).
pub struct InMemoryControl {
    state: Mutex<State>,
    registry: Arc<Mutex<Registry>>,
    next_view_id: AtomicU64,
    next_snapshot_id: AtomicU64,
    /// A kill switch for `node_ping`, so callers (the Identity `Probe` RPC's
    /// unit tests, in particular) can exercise "the engine pod is dead"
    /// without a second `ControlClient` impl. Every other method stays
    /// healthy when this is set: only `node.ping` models a pod that stopped
    /// answering.
    unreachable: std::sync::atomic::AtomicBool,
    /// How many of the next `quota_set` calls fail the way the real one does
    /// under concurrent metadata load (plan 37 "K0 results" Track B: the
    /// whole-journal round-waiter's `journal not shipped: no lease`).
    quota_set_failures: AtomicU32,
    /// Every `quota_set` call, failed or not.
    quota_set_calls: AtomicU64,
    /// The filesystem this engine serves, as `fs.list` reports it (its
    /// unnamed entry); `None`: it reports none.
    own_uuid: Mutex<Option<String>>,
    /// Stand in for a filesystem whose volumes somebody else created:
    /// `browse.xattr` on a missing path creates it first (the node
    /// plugin's in-memory backend, whose controller is another process).
    any_path: std::sync::atomic::AtomicBool,
    /// How many descriptors `view.mount{PreopenedFd}` received.
    fds_received: AtomicU64,
    /// How many `fs.unlock` calls arrived.
    unlocks: AtomicU64,
    /// Every subtree usage walk: a `quota_get` of anything but `/` (the
    /// real engine's `recursive_size`, O(entries)).
    usage_walks: AtomicU64,
    /// Every `clone.create` call.
    clones: AtomicU64,
    /// How many of the next `clone.create` calls fail.
    clone_failures: AtomicU32,
    /// Every `snapshot.list` call.
    snapshot_lists: AtomicU64,
    /// Plan 37 §8's socket handoff, as a sender and as a standby.
    handoff: Mutex<FakeHandoff>,
}

/// [`InMemoryControl`]'s side of a socket handoff (plan 37 §8): the
/// daemon's sender phases and its standby's, with injected failures. A
/// standby resumes once it is sealed and its sender has committed — the
/// fake's stand-in for "the state dir's lock is free".
#[derive(Default)]
struct FakeHandoff {
    prepared: Vec<(u64, Mounted)>,
    transferred: bool,
    /// Committed: the process is gone; every later call fails.
    exited: bool,
    /// A standby: the engine it takes over from.
    from: Option<Arc<InMemoryControl>>,
    received: Vec<Mounted>,
    sealed: bool,
    aborted: bool,
    resumed: bool,
    /// Phases that fail (each once), and a resume that fails.
    fail: Vec<HandoffPhase>,
    fail_resume: Option<String>,
    /// The transport `Prepare` reports (default `dev_fuse`).
    transport: Option<String>,
    /// As a standby: how long after its sender's commit it resumes (a
    /// slow engine start), or never.
    resume_after: Option<Duration>,
    resume_never: bool,
    committed_at: Option<std::time::Instant>,
    /// The parameters of every phase asked, the last of each.
    params: Vec<(HandoffPhase, HandoffParams)>,
    /// Its own deadline serves the sessions again just before the commit
    /// arrives: the commit fails, `Status` answers `Serving`.
    deadline_before_commit: bool,
    /// Every socket phase asked, in order.
    calls: Vec<HandoffPhase>,
}

impl Default for InMemoryControl {
    fn default() -> InMemoryControl {
        InMemoryControl::with_registry(Arc::default())
    }
}

impl InMemoryControl {
    fn with_registry(registry: Arc<Mutex<Registry>>) -> InMemoryControl {
        let mut tree = BTreeMap::new();
        tree.insert("/".to_string(), DirEntry::default());
        InMemoryControl {
            state: Mutex::new(State {
                tree,
                snapshots: Vec::new(),
                views: BTreeMap::new(),
            }),
            registry,
            next_view_id: AtomicU64::new(1),
            next_snapshot_id: AtomicU64::new(1),
            unreachable: std::sync::atomic::AtomicBool::new(false),
            quota_set_failures: AtomicU32::new(0),
            quota_set_calls: AtomicU64::new(0),
            usage_walks: AtomicU64::new(0),
            own_uuid: Mutex::new(None),
            any_path: std::sync::atomic::AtomicBool::new(false),
            fds_received: AtomicU64::new(0),
            unlocks: AtomicU64::new(0),
            clones: AtomicU64::new(0),
            clone_failures: AtomicU32::new(0),
            snapshot_lists: AtomicU64::new(0),
            handoff: Mutex::default(),
        }
    }

    /// A standby replacing `old` (plan 37 §8): the same filesystem (its
    /// tree, copied — nothing changes it while the views are stopped), no
    /// view until a handoff is resumed.
    pub fn standby_for(old: &Arc<InMemoryControl>) -> InMemoryControl {
        let c = InMemoryControl::with_registry(old.registry.clone());
        c.state.lock().unwrap().tree = old.state.lock().unwrap().tree.clone();
        *c.own_uuid.lock().unwrap() = old.own_uuid.lock().unwrap().clone();
        c.any_path
            .store(old.any_path.load(Ordering::SeqCst), Ordering::SeqCst);
        c.handoff.lock().unwrap().from = Some(old.clone());
        c
    }

    /// Fail the next `node.handoff` of `phase` (once).
    pub fn fail_handoff(&self, phase: HandoffPhase) {
        self.handoff.lock().unwrap().fail.push(phase);
    }

    /// As a standby: fail the resume (after the sender committed).
    pub fn fail_resume(&self, reason: &str) {
        self.handoff.lock().unwrap().fail_resume = Some(reason.to_string());
    }

    /// As a standby: resume only `after` its sender committed.
    pub fn resume_after(&self, after: Duration) {
        self.handoff.lock().unwrap().resume_after = Some(after);
    }

    /// As a standby: never resume (nor fail) once sealed.
    pub fn resume_never(&self) {
        self.handoff.lock().unwrap().resume_never = true;
    }

    /// The parameters the last `node.handoff` of `phase` was asked with.
    pub fn handoff_params(&self, phase: HandoffPhase) -> Option<HandoffParams> {
        self.handoff
            .lock()
            .unwrap()
            .params
            .iter()
            .rev()
            .find(|(p, _)| *p == phase)
            .map(|(_, params)| params.clone())
    }

    /// Abort by its own deadline just before the commit arrives.
    pub fn deadline_before_commit(&self) {
        self.handoff.lock().unwrap().deadline_before_commit = true;
    }

    /// Report `transport` for every prepared view.
    pub fn report_transport(&self, transport: &str) {
        self.handoff.lock().unwrap().transport = Some(transport.to_string());
    }

    /// The socket handoff phases asked of this engine, in order.
    pub fn handoff_calls(&self) -> Vec<HandoffPhase> {
        self.handoff.lock().unwrap().calls.clone()
    }

    /// Committed and gone.
    pub fn handed_off(&self) -> bool {
        self.handoff.lock().unwrap().exited
    }

    /// Prepared (its sessions stopped) and not yet committed or aborted.
    pub fn handoff_prepared(&self) -> bool {
        !self.handoff.lock().unwrap().prepared.is_empty()
    }

    fn handoff_report(&self, h: &FakeHandoff, state: HandoffState) -> HandoffReport {
        let transport = h.transport.clone().unwrap_or_else(|| "dev_fuse".into());
        let views = if h.from.is_some() {
            h.received
                .iter()
                .map(|m| HandedOffView {
                    mountpoint: m.mountpoint.clone(),
                    transport: transport.clone(),
                    ..Default::default()
                })
                .collect()
        } else {
            h.prepared
                .iter()
                .map(|(id, m)| HandedOffView {
                    id: *id,
                    mountpoint: m.mountpoint.clone(),
                    handles: 0,
                    transport: transport.clone(),
                })
                .collect()
        };
        HandoffReport {
            detail: format!("fake {state:?}"),
            views,
            state: Some(state),
            elapsed_ms: 0,
        }
    }

    /// `node.handoff{Socket}` (the doc of [`FakeHandoff`]).
    fn socket_handoff(
        &self,
        params: HandoffParams,
        fd: Option<OwnedFd>,
    ) -> Result<HandoffReport, ControlError> {
        let mut h = self.handoff.lock().unwrap();
        if h.exited {
            return Err(ControlError::unavailable("the engine pod has exited"));
        }
        let phase = params
            .phase
            .ok_or_else(|| ControlError::invalid("a socket handoff names its phase"))?;
        h.calls.push(phase);
        h.params.push((phase, params.clone()));
        if let Some(i) = h.fail.iter().position(|p| *p == phase) {
            h.fail.remove(i);
            return Err(ControlError::failed(format!("injected {phase:?} failure")));
        }
        if h.from.is_some() {
            return self.standby_handoff(&mut h, phase, fd);
        }
        match phase {
            HandoffPhase::Prepare => {
                if !h.prepared.is_empty() {
                    return Err(ControlError::failed("a handover is already under way"));
                }
                let views: Vec<(u64, Mounted)> = self
                    .state
                    .lock()
                    .unwrap()
                    .views
                    .iter()
                    .map(|(id, m)| (*id, m.clone()))
                    .collect();
                if views.is_empty() {
                    return Err(ControlError::failed("no view is mounted"));
                }
                h.prepared = views;
                Ok(self.handoff_report(&h, HandoffState::Prepared))
            }
            HandoffPhase::Transfer => {
                if h.prepared.is_empty() {
                    return Err(ControlError::invalid("nothing is prepared"));
                }
                let fd = fd.ok_or_else(|| ControlError::invalid("transfer needs a socket"))?;
                let mut sock = std::os::unix::net::UnixStream::from(fd);
                let io = |e: std::io::Error| ControlError::failed(e.to_string());
                for (_, m) in &h.prepared {
                    let record = serde_json::to_vec(m).expect("a view record");
                    let conn = std::fs::File::open("/dev/null").map_err(io)?;
                    constellation_control::handoff_wire::write_record(
                        &mut sock,
                        &record,
                        std::os::fd::AsFd::as_fd(&conn),
                    )
                    .map_err(io)?;
                }
                constellation_control::handoff_wire::write_end(&mut sock).map_err(io)?;
                h.transferred = true;
                Ok(self.handoff_report(&h, HandoffState::Transferred))
            }
            HandoffPhase::Commit => {
                if h.deadline_before_commit {
                    h.prepared.clear();
                    h.transferred = false;
                    return Err(ControlError::invalid("nothing is prepared to commit"));
                }
                if !h.transferred {
                    return Err(ControlError::invalid("nothing was transferred"));
                }
                let report = self.handoff_report(&h, HandoffState::Committed);
                h.prepared.clear();
                h.exited = true;
                h.committed_at = Some(std::time::Instant::now());
                self.state.lock().unwrap().views.clear();
                self.unreachable.store(true, Ordering::SeqCst);
                Ok(report)
            }
            HandoffPhase::Abort => {
                h.prepared.clear();
                h.transferred = false;
                Ok(self.handoff_report(&h, HandoffState::Serving))
            }
            HandoffPhase::Status => {
                let state = match (h.prepared.is_empty(), h.transferred) {
                    (true, _) => HandoffState::Serving,
                    (false, false) => HandoffState::Prepared,
                    (false, true) => HandoffState::Transferred,
                };
                Ok(self.handoff_report(&h, state))
            }
            HandoffPhase::Receive | HandoffPhase::Seal => Err(ControlError::invalid(
                "this engine is serving, not a standby",
            )),
        }
    }

    fn standby_handoff(
        &self,
        h: &mut FakeHandoff,
        phase: HandoffPhase,
        fd: Option<OwnedFd>,
    ) -> Result<HandoffReport, ControlError> {
        match phase {
            HandoffPhase::Receive => {
                if h.sealed || h.aborted {
                    return Err(ControlError::invalid("this standby is sealed"));
                }
                let fd = fd.ok_or_else(|| ControlError::invalid("receive needs the stream"))?;
                let mut sock = std::os::unix::net::UnixStream::from(fd);
                let io = |e: std::io::Error| ControlError::failed(e.to_string());
                let mut records = Vec::new();
                while let Some((record, _conn)) =
                    constellation_control::handoff_wire::read_record(&mut sock).map_err(io)?
                {
                    let record: Mounted = serde_json::from_slice(&record)
                        .map_err(|e| ControlError::invalid(format!("an unreadable record: {e}")))?;
                    records.push(record);
                }
                self.fds_received
                    .fetch_add(records.len() as u64, Ordering::SeqCst);
                h.received.extend(records);
                let received = h.received.len() as u64;
                Ok(self.handoff_report(h, HandoffState::Standby { received }))
            }
            HandoffPhase::Seal => {
                if h.received.is_empty() {
                    return Err(ControlError::invalid("nothing was received to seal"));
                }
                h.sealed = true;
                Ok(self.handoff_report(h, HandoffState::Sealed))
            }
            HandoffPhase::Abort => {
                if h.resumed {
                    return Err(ControlError::invalid("too late to abort"));
                }
                h.aborted = true;
                h.received.clear();
                Ok(self.handoff_report(
                    h,
                    HandoffState::Failed {
                        reason: "aborted by request".into(),
                    },
                ))
            }
            HandoffPhase::Status => {
                let committed_at = h
                    .from
                    .as_ref()
                    .and_then(|from| from.handoff.lock().unwrap().committed_at);
                let committed = committed_at.is_some_and(|at| {
                    !h.resume_never && at.elapsed() >= h.resume_after.unwrap_or_default()
                });
                let state = if h.aborted {
                    HandoffState::Failed {
                        reason: "aborted by request".into(),
                    }
                } else if h.resumed {
                    HandoffState::Resumed { failed: Vec::new() }
                } else if h.sealed && committed {
                    if let Some(reason) = h.fail_resume.clone() {
                        HandoffState::Failed { reason }
                    } else {
                        let mut state = self.state.lock().unwrap();
                        for m in h.received.drain(..) {
                            let id = self.next_view_id.fetch_add(1, Ordering::SeqCst);
                            state.views.insert(id, m);
                        }
                        h.resumed = true;
                        HandoffState::Resumed { failed: Vec::new() }
                    }
                } else if h.sealed {
                    HandoffState::Sealed
                } else {
                    HandoffState::Standby {
                        received: h.received.len() as u64,
                    }
                };
                Ok(self.handoff_report(h, state))
            }
            _ => Err(ControlError::invalid("a standby only receives")),
        }
    }

    /// An engine serving filesystem `uuid` (what `fs.list` reports).
    pub fn serving(uuid: &str) -> InMemoryControl {
        let c = InMemoryControl::default();
        *c.own_uuid.lock().unwrap() = Some(uuid.to_string());
        c
    }

    /// From now on, `browse.xattr` on a missing path creates it (see the
    /// field).
    pub fn accept_any_path(&self) {
        self.any_path.store(true, Ordering::SeqCst);
    }

    /// Create `path` and its ancestors (a test planting a volume).
    pub fn plant_dir(&self, path: &str) {
        let path = normalize(path);
        let mut state = self.state.lock().unwrap();
        let mut built = String::new();
        for part in path.trim_matches('/').split('/').filter(|p| !p.is_empty()) {
            built.push('/');
            built.push_str(part);
            state.tree.entry(built.clone()).or_default();
        }
    }

    /// Remove `path` and everything under it (a human's `rm -rf`).
    pub fn remove_tree(&self, path: &str) {
        let path = normalize(path);
        let mut state = self.state.lock().unwrap();
        for p in InMemoryControl::subtree_of(&state.tree, &path) {
            state.tree.remove(&p);
        }
    }

    /// The mountpoints of the views mounted now.
    pub fn view_mountpoints(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        state.views.values().map(|v| v.mountpoint.clone()).collect()
    }

    /// Every view goes, as when the engine process serving them dies.
    pub fn drop_views(&self) {
        self.state.lock().unwrap().views.clear();
    }

    /// How many `fs.unlock` calls arrived.
    pub fn unlocks(&self) -> u64 {
        self.unlocks.load(Ordering::SeqCst)
    }

    /// How many descriptors `view.mount{PreopenedFd}` has received.
    pub fn fds_received(&self) -> u64 {
        self.fds_received.load(Ordering::SeqCst)
    }

    /// From the next `node_ping` on, answer as if the engine pod were
    /// unreachable instead of healthy.
    pub fn mark_unreachable(&self) {
        self.unreachable.store(true, Ordering::SeqCst);
    }

    /// Fail the next `n` `quota_set` calls with the engine's transient
    /// barrier error (`ErrorKind::Failed`, "journal not shipped: no lease").
    pub fn fail_next_quota_sets(&self, n: u32) {
        self.quota_set_failures.store(n, Ordering::SeqCst);
    }

    /// How many subtree usage walks callers have asked for (`quota_get`
    /// on a subtree), the cost the real engine pays per entry.
    pub fn usage_walks(&self) -> u64 {
        self.usage_walks.load(Ordering::SeqCst)
    }

    /// Fail the next `n` `clone.create` calls.
    pub fn fail_next_clones(&self, n: u32) {
        self.clone_failures.store(n, Ordering::SeqCst);
    }

    /// How many `clone.create` calls arrived.
    pub fn clones(&self) -> u64 {
        self.clones.load(Ordering::SeqCst)
    }

    /// How many `snapshot.list` calls arrived.
    pub fn snapshot_lists(&self) -> u64 {
        self.snapshot_lists.load(Ordering::SeqCst)
    }

    /// Every snapshot this filesystem holds, as `snapshot.list` reports it.
    pub fn snapshots(&self) -> Vec<SnapshotStatus> {
        let state = self.state.lock().unwrap();
        state.snapshots.iter().map(|s| s.status.clone()).collect()
    }

    /// How many times `quota_set` has been called, failures included.
    pub fn quota_set_calls(&self) -> u64 {
        self.quota_set_calls.load(Ordering::SeqCst)
    }

    /// Plant `bytes` of usage on `path` (this fake stores no file data), so
    /// a test can drive the "shrink below what is used" paths.
    pub fn set_used_bytes(&self, path: &str, bytes: u64) {
        let path = normalize(path);
        let mut state = self.state.lock().unwrap();
        if let Some(entry) = state.tree.get_mut(&path) {
            entry.used_bytes = bytes;
        }
    }

    /// Whether `path` exists in this filesystem's tree.
    pub fn exists(&self, path: &str) -> bool {
        self.state
            .lock()
            .unwrap()
            .tree
            .contains_key(&normalize(path))
    }

    /// Every path directly under `dir`.
    pub fn children(&self, dir: &str) -> Vec<String> {
        let dir = normalize(dir);
        let state = self.state.lock().unwrap();
        state
            .tree
            .keys()
            .filter(|p| p.as_str() != dir && parent_of(p).as_deref() == Some(dir.as_str()))
            .cloned()
            .collect()
    }

    /// Whether `uuid` names a filesystem `fs.create` has registered.
    fn knows_filesystem(&self, uuid: &str) -> bool {
        self.registry
            .lock()
            .unwrap()
            .filesystems
            .values()
            .any(|u| u == uuid)
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

/// The engine's `split_selector`: `path@name`, split at the last `@`.
fn split_selector(selector: &str) -> Result<(String, String), ControlError> {
    let (path, name) = selector.rsplit_once('@').ok_or_else(|| {
        ControlError::failed(format!(
            "snapshot selector {selector:?} must be <path>@<name>"
        ))
    })?;
    if name.is_empty() || name.contains('/') {
        return Err(ControlError::failed(
            "snapshot name must be non-empty and contain neither '/' nor '@'",
        ));
    }
    Ok((normalize(path), name.to_string()))
}

/// The engine's hold-owner namespaces (`user:`, `csi:`).
fn validate_owner(by: &str) -> Result<(), ControlError> {
    match by.split_once(':') {
        Some(("user" | "csi", rest)) if !rest.is_empty() => Ok(()),
        _ => Err(ControlError::invalid(format!(
            "hold owner {by:?} needs a namespace: `user:<name>` or `csi:<id>`"
        ))),
    }
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
        let mut registry = self.registry.lock().unwrap();
        let key = (params.bucket.clone(), params.prefix.clone());
        if let Some(uuid) = registry.filesystems.get(&key) {
            return Ok(FsCreated {
                uuid: uuid.clone(),
                created: false,
            });
        }
        registry.next_fs_uuid += 1;
        let uuid = format!("fake-fs-{:08x}", registry.next_fs_uuid);
        registry.filesystems.insert(key, uuid.clone());
        Ok(FsCreated {
            uuid,
            created: true,
        })
    }

    async fn fs_unlock(&self, _params: FsUnlockParams) -> Result<Ack, ControlError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
        Ok(Ack::new("unlocked"))
    }

    async fn fs_list(&self) -> Result<FsListing, ControlError> {
        let mut filesystems: Vec<FsInfo> = self
            .registry
            .lock()
            .unwrap()
            .filesystems
            .iter()
            .map(|((bucket, prefix), uuid)| FsInfo {
                uuid: uuid.clone(),
                name: Some(format!("{bucket}/{prefix}")),
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                ..Default::default()
            })
            .collect();
        if let Some(uuid) = self.own_uuid.lock().unwrap().clone() {
            filesystems.push(FsInfo {
                uuid,
                name: None,
                unlocked: true,
                ..Default::default()
            });
        }
        Ok(FsListing { filesystems })
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
        // Like the engine's `browse.mkdir`: an existing directory is fine
        // with `parents` (`mkdir -p`), `EEXIST` without.
        if state.tree.contains_key(&path) && !params.parents {
            return Err(ControlError::from(constellation_types::Code::Exists));
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
        if self.any_path.load(Ordering::SeqCst) {
            self.plant_dir(&path);
        }
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
        // `rename(2)` never creates the destination's parent.
        let to_parent = parent_of(&to).unwrap_or_else(|| "/".to_string());
        if !state.tree.contains_key(&to_parent) {
            return Err(ControlError::not_found(format!(
                "{to_parent} does not exist"
            )));
        }
        if to == from || to.starts_with(&format!("{from}/")) {
            return Err(ControlError::invalid(format!(
                "cannot move {from} under itself ({to})"
            )));
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

    async fn quota_get(&self, subtree: &str) -> Result<QuotaStatus, ControlError> {
        let path = normalize(subtree);
        let state = self.state.lock().unwrap();
        let entry = state
            .tree
            .get(&path)
            .ok_or_else(|| ControlError::not_found(format!("{path} does not exist")))?;
        if path != "/" {
            self.usage_walks.fetch_add(1, Ordering::SeqCst);
        }
        let used_bytes = InMemoryControl::subtree_of(&state.tree, &path)
            .iter()
            .map(|p| state.tree[p].used_bytes)
            .sum();
        Ok(QuotaStatus {
            max_bytes: entry.quota,
            used_bytes,
        })
    }

    async fn quota_set(&self, params: SubtreeQuotaParams) -> Result<QuotaStatus, ControlError> {
        self.quota_set_calls.fetch_add(1, Ordering::SeqCst);
        let injected = self
            .quota_set_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if injected {
            return Err(ControlError::failed("journal not shipped: no lease"));
        }
        let path = normalize(&params.subtree);
        {
            let mut state = self.state.lock().unwrap();
            let entry = state
                .tree
                .get_mut(&path)
                .ok_or_else(|| ControlError::not_found(format!("{path} does not exist")))?;
            entry.quota = params.max_bytes;
        }
        if path == "/" {
            return self.quota_get(&path).await;
        }
        // Like the engine: a subtree's cap change does not walk it.
        Ok(QuotaStatus {
            max_bytes: params.max_bytes,
            used_bytes: 0,
        })
    }

    async fn quota_cap(&self, subtree: &str) -> Result<Option<u64>, ControlError> {
        let path = normalize(subtree);
        let state = self.state.lock().unwrap();
        state
            .tree
            .get(&path)
            .map(|entry| entry.quota)
            .ok_or_else(|| ControlError::not_found(format!("{path} does not exist")))
    }

    /// Like the engine's: `selector` is `path@name`, the path must exist,
    /// a taken name is refused, and every refusal is `Failed` with a
    /// message (the engine maps all of them through one `failed`). The
    /// snapshot freezes a copy of the subtree, so clones see it as it was.
    async fn snapshot_create(
        &self,
        params: SnapshotCreateParams,
    ) -> Result<SnapshotCreated, ControlError> {
        let (held, owner) = params.hold_request().map_err(ControlError::invalid)?;
        if let Some(by) = owner {
            validate_owner(by)?;
        }
        let (path, name) = split_selector(&params.selector)?;
        if name.contains(['%', '*']) {
            return Err(ControlError::failed(
                "snapshot name must be non-empty and contain none of '/', '@', '%', '*'",
            ));
        }
        let mut state = self.state.lock().unwrap();
        if !state.tree.contains_key(&path) {
            return Err(ControlError::failed(format!("{path}: not found")));
        }
        if state
            .snapshots
            .iter()
            .any(|s| s.status.path == path && s.status.name == name)
        {
            return Err(ControlError::failed(format!(
                "snapshot {path}@{name} already exists"
            )));
        }
        let under = InMemoryControl::subtree_of(&state.tree, &path);
        let refer: u64 = under.iter().map(|p| state.tree[p].used_bytes).sum();
        let frozen = under
            .iter()
            .map(|p| {
                let rel = if path == "/" {
                    p.clone()
                } else {
                    normalize(&p[path.len()..])
                };
                (rel, state.tree[p].clone())
            })
            .collect();
        let id = format!(
            "fakesnap{:08x}",
            self.next_snapshot_id.fetch_add(1, Ordering::SeqCst)
        );
        let status = SnapshotStatus {
            id,
            path: path.clone(),
            name: name.clone(),
            created_unix_ms: unix_ms(),
            held,
            held_by: owner.map(str::to_string),
            origin: "manual".to_string(),
            refer_bytes: Some(refer),
            ..Default::default()
        };
        state.snapshots.push(FrozenSnapshot {
            status: status.clone(),
            frozen,
        });
        Ok(SnapshotCreated {
            detail: format!("created snapshot {path}@{name}"),
            snapshot: status,
        })
    }

    async fn snapshot_delete(&self, params: SnapshotDeleteParams) -> Result<Ack, ControlError> {
        let (path, name) = split_selector(&params.selector)?;
        let mut state = self.state.lock().unwrap();
        let idx = state
            .snapshots
            .iter()
            .position(|s| s.status.path == path && s.status.name == name)
            .ok_or_else(|| {
                ControlError::failed(format!("snapshot {path}@{name} does not exist"))
            })?;
        let status = &state.snapshots[idx].status;
        if status.held && !params.force {
            return Err(ControlError::failed(format!(
                "snapshot {path}@{name} is held by {}; `snapshot release` first (or pass --force)",
                status.held_by.as_deref().unwrap_or("a plain hold")
            )));
        }
        state.snapshots.remove(idx);
        Ok(Ack::new(format!("deleted snapshot {path}@{name}")))
    }

    async fn snapshot_list(
        &self,
        params: SnapshotListParams,
    ) -> Result<SnapshotListing, ControlError> {
        self.snapshot_lists.fetch_add(1, Ordering::SeqCst);
        let state = self.state.lock().unwrap();
        let path = params.path.as_deref().map(normalize);
        let snapshots = state
            .snapshots
            .iter()
            .map(|s| &s.status)
            .filter(|s| path.as_deref().is_none_or(|p| s.path == p))
            .cloned()
            .collect();
        Ok(SnapshotListing { snapshots })
    }

    /// The engine's owner rule: a hold recorded under an owner is changed
    /// only by that owner (a plain hold by a plain release), unless
    /// `force`.
    async fn snapshot_hold(
        &self,
        params: SnapshotHoldParams,
    ) -> Result<SnapshotHeld, ControlError> {
        let by = params.by.filter(|b| !b.is_empty());
        if let Some(by) = &by {
            validate_owner(by)?;
        }
        let selected = match params.id.contains('@') {
            true => Some(split_selector(&params.id)?),
            false => None,
        };
        let mut state = self.state.lock().unwrap();
        let snapshot = state
            .snapshots
            .iter_mut()
            .map(|s| &mut s.status)
            .find(|s| match &selected {
                Some((path, name)) => &s.path == path && &s.name == name,
                None => s.id == params.id,
            })
            .ok_or_else(|| ControlError::failed(format!("no such snapshot: {}", params.id)))?;
        if snapshot.held && !params.force && snapshot.held_by != by {
            return Err(ControlError::failed(format!(
                "snapshot {}@{} is held by {}, not {}",
                snapshot.path,
                snapshot.name,
                snapshot.held_by.as_deref().unwrap_or("a plain hold"),
                by.as_deref().unwrap_or("a plain hold")
            )));
        }
        snapshot.held = params.held;
        snapshot.held_by = if params.held { by } else { None };
        Ok(SnapshotHeld {
            detail: format!("held: {}", snapshot.held),
            snapshot: snapshot.clone(),
        })
    }

    /// Like the engine's: from a snapshot (`path@name`) only, into a
    /// destination that does not exist yet under a parent that does. The
    /// copy is the frozen tree, xattrs included (the source volume's record
    /// comes along, as it does from the engine); quotas do not travel.
    async fn clone_create(&self, params: CloneParams) -> Result<Ack, ControlError> {
        self.clones.fetch_add(1, Ordering::SeqCst);
        if self
            .clone_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(ControlError::failed("injected clone.create failure"));
        }
        let (path, name) = split_selector(&params.selector)?;
        let to = normalize(&params.destination);
        let mut state = self.state.lock().unwrap();
        let frozen = state
            .snapshots
            .iter()
            .find(|s| s.status.path == path && s.status.name == name)
            .map(|s| s.frozen.clone())
            .ok_or_else(|| {
                ControlError::failed(format!("snapshot {path}@{name} does not exist"))
            })?;
        if state.tree.contains_key(&to) {
            return Err(ControlError::failed(format!("{to}: already exists")));
        }
        let parent = parent_of(&to).unwrap_or_else(|| "/".to_string());
        if !state.tree.contains_key(&parent) {
            return Err(ControlError::failed(format!("{parent}: not found")));
        }
        for (rel, mut entry) in frozen {
            entry.quota = None;
            state
                .tree
                .insert(InMemoryControl::remap("/", &to, &rel), entry);
        }
        Ok(Ack::new(format!("cloned {path}@{name} to {to}")))
    }

    async fn view_mount(&self, params: ViewMountParams) -> Result<ViewInfo, ControlError> {
        let id = self.next_view_id.fetch_add(1, Ordering::SeqCst);
        let mountpoint = match &params.source {
            MountSource::Path { mountpoint, .. } => mountpoint.display().to_string(),
            MountSource::PreopenedFd {
                mountpoint: Some(mountpoint),
                ..
            } => mountpoint.display().to_string(),
            MountSource::PreopenedFd {
                mountpoint: None, ..
            } => format!("fd:{id}"),
        };
        let mut state = self.state.lock().unwrap();
        // Like the daemon: one view per name.
        if state.views.values().any(|v| v.mountpoint == mountpoint) {
            return Err(ControlError::failed(format!(
                "a view is already mounted at {mountpoint}"
            )));
        }
        let subtree = normalize(&params.subtree);
        if !state.tree.contains_key(&subtree) {
            return Err(ControlError::not_found(format!("{subtree} does not exist")));
        }
        state.views.insert(
            id,
            Mounted {
                mountpoint: mountpoint.clone(),
                subtree,
                labels: params.labels.clone(),
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

    async fn view_mount_fd(
        &self,
        params: ViewMountParams,
        fd: OwnedFd,
    ) -> Result<ViewInfo, ControlError> {
        if !matches!(params.source, MountSource::PreopenedFd { .. }) {
            return Err(ControlError::invalid(
                "a descriptor goes with a PreopenedFd source only",
            ));
        }
        self.fds_received.fetch_add(1, Ordering::SeqCst);
        drop(fd);
        self.view_mount(params).await
    }

    async fn view_list(&self, params: ViewListParams) -> Result<ViewListing, ControlError> {
        let state = self.state.lock().unwrap();
        let views = state
            .views
            .iter()
            .filter(|(_, v)| {
                params
                    .labels
                    .iter()
                    .all(|(k, val)| v.labels.get(k) == Some(val))
            })
            .map(|(id, v)| ViewInfo {
                id: *id,
                subtree: v.subtree.clone(),
                mountpoint: v.mountpoint.clone(),
                labels: v.labels.clone(),
                ..Default::default()
            })
            .collect();
        Ok(ViewListing { views })
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
        let named = params.mountpoint.as_ref().map(|m| m.display().to_string());
        let (id, view) = state
            .views
            .iter()
            .find(|(id, v)| match (&params.id, &named) {
                (Some(want), _) => *id == want,
                (None, Some(m)) => &v.mountpoint == m,
                (None, None) => true,
            })
            .ok_or_else(|| ControlError::not_found("no such view"))?;
        let under = InMemoryControl::subtree_of(&state.tree, &view.subtree);
        let used: u64 = under.iter().map(|p| state.tree[p].used_bytes).sum();
        let root = &state.tree["/"];
        Ok(ViewStatsReport {
            id: *id,
            block_size: 4096,
            total_bytes: root.quota.unwrap_or(u64::MAX),
            used_bytes: used,
            available_bytes: root
                .quota
                .map(|m| m.saturating_sub(used))
                .unwrap_or(u64::MAX),
            inodes_total: 1 << 32,
            inodes_used: state.tree.len() as u64,
            rsize: used,
            rcount: under.len() as u64,
        })
    }

    async fn node_ping(&self) -> Result<Pong, ControlError> {
        if self.unreachable.load(Ordering::SeqCst) {
            return Err(ControlError::unavailable("engine pod is unreachable"));
        }
        Ok(Pong {})
    }

    async fn node_handoff(&self, params: HandoffParams) -> Result<HandoffReport, ControlError> {
        if params.target == HandoffTarget::Socket {
            return self.socket_handoff(params, None);
        }
        Ok(HandoffReport {
            detail: "fake handoff".to_string(),
            views: params
                .views
                .into_iter()
                .map(|id| HandedOffView {
                    id,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }

    async fn node_handoff_fd(
        &self,
        params: HandoffParams,
        fd: OwnedFd,
    ) -> Result<HandoffReport, ControlError> {
        if params.target != HandoffTarget::Socket {
            return Err(ControlError::invalid(
                "only a socket handoff carries a descriptor",
            ));
        }
        self.socket_handoff(params, Some(fd))
    }

    async fn node_leave(&self, _params: LeaveParams) -> Result<Ack, ControlError> {
        Ok(Ack::new("left"))
    }
}

/// An [`Engines`] over [`InMemoryControl`]s: one shared filesystem
/// registry, and one independent tree per registered filesystem — the shape
/// of N pool (or shard) filesystems each behind its own engine pod. Backs
/// the controller's unit tests and `constellation-csi --in-memory-backend`
/// (what `tests/csi/sanity.sh` runs `csi-sanity` against).
///
/// It also models which engine pods are up: [`Engines::filesystem`] starts
/// one (counted, [`Self::starts`]), [`Self::stop_all`] is a teardown that
/// deletes them, and the filesystems' trees survive it the way the data in
/// S3 survives its pods. The CO's objects are modelled only when a test
/// says which handles exist ([`Self::set_named`]); otherwise every handle
/// is named, as there is no CO to ask.
#[derive(Default)]
pub struct InMemoryEngines {
    registry: Arc<InMemoryControl>,
    filesystems: Mutex<BTreeMap<String, Arc<InMemoryControl>>>,
    running: Mutex<BTreeSet<String>>,
    starts: AtomicU64,
    retires: AtomicU64,
    named: Mutex<Option<BTreeSet<String>>>,
}

impl InMemoryEngines {
    /// The concrete client for `uuid`, if one has been handed out — for
    /// tests that inject faults or inspect a pool's tree.
    pub fn filesystem_client(&self, uuid: &str) -> Option<Arc<InMemoryControl>> {
        self.filesystems.lock().unwrap().get(uuid).cloned()
    }

    /// The registry client, for tests.
    pub fn registry_client(&self) -> Arc<InMemoryControl> {
        self.registry.clone()
    }

    /// How many engine pods have been started (a filesystem asked for
    /// while none served it).
    pub fn starts(&self) -> u64 {
        self.starts.load(Ordering::SeqCst)
    }

    /// How many [`Engines::retire`] calls stopped a pod.
    pub fn retires(&self) -> u64 {
        self.retires.load(Ordering::SeqCst)
    }

    /// Whether an engine pod serves `uuid` now.
    pub fn is_running(&self, uuid: &str) -> bool {
        self.running.lock().unwrap().contains(uuid)
    }

    /// Delete every engine pod (a teardown); the data stays.
    pub fn stop_all(&self) {
        self.running.lock().unwrap().clear();
    }

    /// From now on the CO has exactly these objects (`None`: no CO).
    pub fn set_named(&self, handles: Option<BTreeSet<String>>) {
        *self.named.lock().unwrap() = handles;
    }
}

#[async_trait]
impl Engines for InMemoryEngines {
    /// The shared registry client: the fake has no per-pool engine, and
    /// `fs.create` through it registers the pool.
    async fn pool(&self, _pool: &PoolRef) -> Result<Arc<dyn ControlClient>, ControlError> {
        Ok(self.registry.clone())
    }

    async fn filesystem(&self, fs_uuid: &str) -> Result<Arc<dyn ControlClient>, ControlError> {
        if !self.registry.knows_filesystem(fs_uuid) {
            return Err(ControlError::not_found(format!(
                "no filesystem {fs_uuid} is registered"
            )));
        }
        if self.running.lock().unwrap().insert(fs_uuid.to_string()) {
            self.starts.fetch_add(1, Ordering::SeqCst);
        }
        let client = self
            .filesystems
            .lock()
            .unwrap()
            .entry(fs_uuid.to_string())
            .or_insert_with(|| {
                Arc::new(InMemoryControl::with_registry(
                    self.registry.registry.clone(),
                ))
            })
            .clone();
        Ok(client)
    }

    async fn running(&self, fs_uuid: &str) -> Result<Option<Arc<dyn ControlClient>>, ControlError> {
        if !self.is_running(fs_uuid) {
            return Ok(None);
        }
        Ok(self
            .filesystem_client(fs_uuid)
            .map(|c| c as Arc<dyn ControlClient>))
    }

    async fn named(&self, handle: Handle<'_>) -> Result<bool, ControlError> {
        let (Handle::Volume(h) | Handle::Snapshot(h)) = handle;
        Ok(self
            .named
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|set| set.contains(h)))
    }

    async fn retire(&self, fs_uuid: &str) -> Result<(), ControlError> {
        if self.running.lock().unwrap().remove(fs_uuid) {
            self.retires.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn running_filesystems(&self) -> Result<Vec<String>, ControlError> {
        Ok(self.running.lock().unwrap().iter().cloned().collect())
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

        // `rename(2)` does not create the destination's parent.
        let no_parent = c
            .browse_rename(RenameParams {
                from: "/volumes/pv1".into(),
                to: "/.trash/pv1-1".into(),
                overwrite: false,
            })
            .await
            .unwrap_err();
        assert_eq!(no_parent.kind, ErrorKind::NotFound);
        c.browse_mkdir(MkdirParams {
            path: "/.trash".into(),
            mode: None,
            parents: false,
        })
        .await
        .unwrap();
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
        assert_eq!(c.quota_get("/").await.unwrap().max_bytes, None);
        let status = c
            .quota_set(SubtreeQuotaParams {
                subtree: "/".into(),
                max_bytes: Some(1024),
            })
            .await
            .unwrap();
        assert_eq!(status.max_bytes, Some(1024));
        assert_eq!(c.quota_get("/").await.unwrap().max_bytes, Some(1024));
    }

    /// Subtree quotas are per directory, travel with a rename, and a
    /// missing subtree is `NotFound` for both get and set.
    #[tokio::test]
    async fn subtree_quota_follows_its_directory() {
        let c = InMemoryControl::default();
        for p in ["/volumes/pv1", "/.trash"] {
            c.browse_mkdir(MkdirParams {
                path: p.into(),
                mode: None,
                parents: true,
            })
            .await
            .unwrap();
        }
        c.quota_set(SubtreeQuotaParams {
            subtree: "/volumes/pv1".into(),
            max_bytes: Some(10),
        })
        .await
        .unwrap();
        c.set_used_bytes("/volumes/pv1", 7);
        let got = c.quota_get("/volumes/pv1").await.unwrap();
        assert_eq!((got.max_bytes, got.used_bytes), (Some(10), 7));
        assert_eq!(c.quota_get("/").await.unwrap().max_bytes, None);
        assert_eq!(c.quota_get("/").await.unwrap().used_bytes, 7);

        c.browse_rename(RenameParams {
            from: "/volumes/pv1".into(),
            to: "/.trash/pv1-1".into(),
            overwrite: false,
        })
        .await
        .unwrap();
        let moved = c.quota_get("/.trash/pv1-1").await.unwrap();
        assert_eq!(moved.max_bytes, Some(10));
        let gone = c.quota_get("/volumes/pv1").await.unwrap_err();
        assert_eq!(gone.kind, ErrorKind::NotFound);
        let gone = c
            .quota_set(SubtreeQuotaParams {
                subtree: "/volumes/pv1".into(),
                max_bytes: Some(1),
            })
            .await
            .unwrap_err();
        assert_eq!(gone.kind, ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn injected_quota_set_failures_are_transient() {
        let c = InMemoryControl::default();
        c.fail_next_quota_sets(2);
        let set = || {
            c.quota_set(SubtreeQuotaParams {
                subtree: "/".into(),
                max_bytes: Some(1),
            })
        };
        assert_eq!(set().await.unwrap_err().kind, ErrorKind::Failed);
        assert_eq!(set().await.unwrap_err().kind, ErrorKind::Failed);
        set().await.unwrap();
        assert_eq!(c.quota_set_calls(), 3);
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
        c.set_used_bytes("/volumes/pv1", 7);

        let created = c
            .snapshot_create(SnapshotCreateParams {
                selector: "/volumes/pv1@s1".into(),
                held_by: Some("csi:content-1".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(created.snapshot.held);
        assert_eq!(created.snapshot.held_by.as_deref(), Some("csi:content-1"));
        assert_eq!(created.snapshot.refer_bytes, Some(7));
        // A taken name, and a missing path, are refused like the engine
        // refuses them: `Failed`.
        for selector in ["/volumes/pv1@s1", "/volumes/nope@s2"] {
            let e = c
                .snapshot_create(SnapshotCreateParams {
                    selector: selector.into(),
                    ..Default::default()
                })
                .await
                .unwrap_err();
            assert_eq!(e.kind, ErrorKind::Failed, "{selector}");
        }

        // A held snapshot refuses delete without force; another owner may
        // not release it.
        let refused = c
            .snapshot_delete(SnapshotDeleteParams {
                selector: "/volumes/pv1@s1".into(),
                force: false,
            })
            .await
            .unwrap_err();
        assert!(refused.message.contains("csi:content-1"), "{refused}");
        let wrong_owner = c
            .snapshot_hold(SnapshotHoldParams {
                id: created.snapshot.id.clone(),
                held: false,
                by: Some("user:x".into()),
                force: false,
            })
            .await
            .unwrap_err();
        assert_eq!(wrong_owner.kind, ErrorKind::Failed);

        // Release the hold (by id), then delete cleanly (by selector).
        c.snapshot_hold(SnapshotHoldParams {
            id: created.snapshot.id.clone(),
            held: false,
            by: Some("csi:content-1".into()),
            force: false,
        })
        .await
        .unwrap();
        c.snapshot_delete(SnapshotDeleteParams {
            selector: "/volumes/pv1@s1".into(),
            force: false,
        })
        .await
        .unwrap();
        let listing = c
            .snapshot_list(SnapshotListParams::default())
            .await
            .unwrap();
        assert!(listing.snapshots.is_empty());
        // A gone snapshot is an error, as from the engine.
        c.snapshot_delete(SnapshotDeleteParams {
            selector: "/volumes/pv1@s1".into(),
            force: false,
        })
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn clone_copies_the_frozen_subtree() {
        let c = InMemoryControl::default();
        c.browse_mkdir(MkdirParams {
            path: "/volumes/source/sub".into(),
            mode: None,
            parents: true,
        })
        .await
        .unwrap();
        let set = |path: &'static str, value: &'static [u8]| {
            c.browse_xattr(XattrParams {
                path: path.into(),
                op: XattrOp::Set {
                    name: "user.constellation.csi.pv".into(),
                    value: value.to_vec().into(),
                },
            })
        };
        set("/volumes/source", b"source").await.unwrap();
        c.snapshot_create(SnapshotCreateParams {
            selector: "/volumes/source@s".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        // Changed after the snapshot: the clone does not see it.
        set("/volumes/source", b"changed").await.unwrap();
        // Only from a snapshot.
        c.clone_create(CloneParams {
            selector: "/volumes/source".into(),
            destination: "/volumes/clone".into(),
        })
        .await
        .unwrap_err();
        c.clone_create(CloneParams {
            selector: "/volumes/source@s".into(),
            destination: "/volumes/clone".into(),
        })
        .await
        .unwrap();
        assert!(c.exists("/volumes/clone/sub"));
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
        // Never over an existing destination.
        c.clone_create(CloneParams {
            selector: "/volumes/source@s".into(),
            destination: "/volumes/clone".into(),
        })
        .await
        .unwrap_err();
    }

    /// `/@name` is how every `layout: dedicated` snapshot names its source
    /// (the whole tree). Every other path must land one level under the
    /// destination, with the separator intact, and the root's own entry
    /// must become the destination itself.
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
        c.snapshot_create(SnapshotCreateParams {
            selector: "/@all".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        c.clone_create(CloneParams {
            selector: "/@all".into(),
            destination: "/volumes/clone".into(),
        })
        .await
        .unwrap();

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
        for p in [
            "/volumes/source",
            "/volumes/clone/volumes/source",
            "/volumes/clone/volumes/source/sub",
            "/volumes/clone/other",
        ] {
            assert!(c.exists(p), "{p} should exist after cloning /");
        }
        assert!(!c.exists("/volumes/clonevolumes"));
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
        let params = ViewMountParams {
            subtree: "/volumes/pv1".into(),
            source: MountSource::PreopenedFd {
                mountpoint: Some("/staging/pv1".into()),
                opts: Default::default(),
            },
            labels: Default::default(),
            qos: Default::default(),
            confine_links: false,
        };
        let fd = std::fs::File::open("/dev/null").unwrap().into();
        let missing = c.view_mount_fd(params.clone(), fd).await.unwrap_err();
        assert_eq!(missing.kind, ErrorKind::NotFound, "no such subtree yet");
        c.plant_dir("/volumes/pv1");
        let fd = std::fs::File::open("/dev/null").unwrap().into();
        let view = c.view_mount_fd(params.clone(), fd).await.unwrap();
        assert_eq!(view.subtree, "/volumes/pv1");
        assert_eq!(
            view.mountpoint, "/staging/pv1",
            "known by the sender's name"
        );
        assert_eq!(c.fds_received(), 2);
        let fd = std::fs::File::open("/dev/null").unwrap().into();
        let twice = c.view_mount_fd(params, fd).await.unwrap_err();
        assert_eq!(twice.kind, ErrorKind::Failed, "one view per name");

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
        c.quota_get("/").await.unwrap();
    }

    #[tokio::test]
    async fn engines_share_one_registry_and_keep_trees_apart() {
        let engines = InMemoryEngines::default();
        let registry = engines.registry_client();
        let a = registry
            .fs_create(FsCreateParams {
                bucket: "b".into(),
                prefix: "a".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let b = registry
            .fs_create(FsCreateParams {
                bucket: "b".into(),
                prefix: "b".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let fs_a = engines.filesystem(&a.uuid).await.unwrap();
        // The same (bucket, prefix) through a per-filesystem client: the
        // registry is shared, so the uuid is the same.
        let again = fs_a
            .fs_create(FsCreateParams {
                bucket: "b".into(),
                prefix: "a".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(again.uuid, a.uuid);
        fs_a.browse_mkdir(MkdirParams {
            path: "/volumes".into(),
            mode: None,
            parents: false,
        })
        .await
        .unwrap();
        assert!(engines
            .filesystem_client(&a.uuid)
            .unwrap()
            .exists("/volumes"));
        let fs_b = engines.filesystem(&b.uuid).await.unwrap();
        assert_eq!(
            fs_b.quota_get("/volumes").await.unwrap_err().kind,
            ErrorKind::NotFound
        );
        let unknown = engines.filesystem("nope").await.err().unwrap();
        assert_eq!(unknown.kind, ErrorKind::NotFound);
    }
}
