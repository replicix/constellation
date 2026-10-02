//! The typed method table (plan 31 §9.2): one zero-sized type per control
//! method, carrying its name, minimum role, whether it mutates, whether it
//! streams, and its parameter/result types.
//!
//! ## Why a trait per method
//!
//! The old `dispatch()` was a 37-arm `match` over an enum; adding a method
//! meant touching the enum, the match, the trait and both adapters. Here a
//! method is *declared once* in the `define_methods!` macro below and everything else
//! derives from that one line:
//!
//! - the server registers a handler by type
//!   (`router.register::<PinAdd>(|ctx, p| async { … })`) so parameter and
//!   result types cannot drift from the declaration,
//! - the client calls by type (`client.call::<PinAdd>(params)`),
//! - authorization reads [`Method::MIN_ROLE`] before any handler runs,
//! - the audit log records exactly the [`Method::MUTATING`] ones,
//! - [`METHODS`] and [`visit_all`] enumerate the table for `--help`-style
//!   listings, the schema generator and the parity tests,
//! - the JSON Schema document ([`crate::schema`]) is generated from the
//!   `Params`/`Result` types of the same declaration.
//!
//! Because [`METHODS`], [`visit_all`] and the `impl Method` blocks come out of
//! the same macro invocation, "a method exists but is missing from the table"
//! cannot happen; the tests instead pin the *contents* (65 methods, no
//! duplicates, every one of the 37 old `Request` variants maps to exactly
//! one).
//!
//! ## Streaming methods
//!
//! [`StreamKind::Events`] methods (subscriptions) yield `Method::Event`s;
//! [`StreamKind::Chunks`] methods (bulk data) yield byte chunks. Both end in
//! a terminal `Response` whose `Ok` payload is [`StreamEnd`], so their
//! `Result` type is always `StreamEnd` (the tests enforce it).
//!
//! ## Minimum roles
//!
//! | class | role | methods |
//! |---|---|---|
//! | reads, listings, browsing | viewer | `node.ping/status/logs.tail/ops`, `*.list*`, `snapshot.refs`, `snapshot.policy.check/simulate/list/show`, `quota.get`, `browse.readdir/inspect/stat/read`, `view.stats`, `peers.list`, `stats.subscribe`, `events.subscribe` |
//! | node-local mutation (and probes that write to the backend) | operator | `pin.add/remove`, `designation.offline/online/delegate/undelegate`, `node.reintegrate/set_write_mode/doctor`, `snapshot.create`, `snapshot.hold`, `clone.create`, `cache.prune`, `browse.write/mkdir/rename/xattr`, `fs.doctor` |
//! | destructive or cluster-wide | admin | `node.leave/handoff/lifecycle`, `prune.run`, `gc.run`, `fsck.run`, `snapshot.delete`, `snapshot.policy.set/remove/pause`, `locks.*`, `quota.set`, `view.mount/unmount`, `browse.delete`, `fs.create/import/export/passwd/unlock` |
//!
//! `fsck.run` is admin although a dry run only reads: one method, one role,
//! and the same method repairs and force-releases. `browse.xattr` is
//! operator and audited although `Get`/`List` only read: the role is a
//! property of the method, not of its arguments. `fs.export` is admin but
//! not audited (it changes nothing); it can name credential sources.

use crate::authz::Role;
use crate::proto::types::*;
use crate::proto::{Empty, NoEvent, StreamEnd};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Whether (and how) a method streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    /// One `Request`, one `Response`.
    None,
    /// A subscription: `Event` frames until cancelled or the connection
    /// drops.
    Events,
    /// Bulk data: `Chunk` frames, then the `Response`.
    Chunks,
}

/// The runtime description of a method (see [`METHODS`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MethodInfo {
    pub name: &'static str,
    pub min_role: Role,
    pub mutating: bool,
    pub streaming: StreamKind,
    /// The params carry [`Secret`](crate::proto::Secret)s; see
    /// [`Method::SECRET_PARAMS`].
    pub secret_params: bool,
}

/// A control method. See the [module docs](self).
pub trait Method: Send + Sync + 'static {
    /// Dotted lowercase name, e.g. `pin.add`; the wire identifier.
    const NAME: &'static str;
    /// The least role allowed to call it.
    const MIN_ROLE: Role;
    /// Audited, and counted as a change.
    const MUTATING: bool;
    const STREAMING: StreamKind;
    /// The params carry secrets (passphrases, keys). The audit log then
    /// withholds even their digest: an unsalted hash of a low-entropy
    /// passphrase is a dictionary attack away from the passphrase. The
    /// tests prove this is set exactly for the methods whose params schema
    /// contains a `Secret`.
    const SECRET_PARAMS: bool = false;

    type Params: Serialize + DeserializeOwned + JsonSchema + Send + 'static;
    /// For streaming methods always [`StreamEnd`].
    type Result: Serialize + DeserializeOwned + JsonSchema + Send + 'static;
    /// The `Event` payload of a [`StreamKind::Events`] method; [`NoEvent`]
    /// otherwise.
    type Event: Serialize + DeserializeOwned + JsonSchema + Send + 'static;

    /// Whether these parameters need a file descriptor attached to the
    /// request (`view.mount` with `PreopenedFd`). The router answers
    /// `NotSupported` (transport cannot pass one) or `Invalid` (none was
    /// attached) *before* the handler runs, so a handler can `take_fd()`
    /// without a hang path.
    fn requires_fd(_params: &Self::Params) -> bool {
        false
    }

    const INFO: MethodInfo = MethodInfo {
        name: Self::NAME,
        min_role: Self::MIN_ROLE,
        mutating: Self::MUTATING,
        streaming: Self::STREAMING,
        secret_params: Self::SECRET_PARAMS,
    };
}

/// Callback for [`visit_all`]: sees every method type once.
pub trait MethodVisitor {
    fn visit<M: Method>(&mut self);
}

macro_rules! event_ty {
    () => {
        NoEvent
    };
    ($t:ty) => {
        $t
    };
}

macro_rules! define_methods {
    ($(
        $(#[$meta:meta])*
        $ty:ident {
            name: $name:literal,
            role: $role:ident,
            mutating: $mutating:literal,
            stream: $stream:ident,
            params: $params:ty,
            result: $result:ty
            $(, event: $event:ty)?
            $(, requires_fd: $fd:expr)?
            $(, secret_params: $secret:literal)?
            $(,)?
        }
    )*) => {
        $(
            $(#[$meta])*
            #[derive(Debug, Clone, Copy, Default)]
            pub struct $ty;

            impl Method for $ty {
                const NAME: &'static str = $name;
                const MIN_ROLE: Role = Role::$role;
                const MUTATING: bool = $mutating;
                const STREAMING: StreamKind = StreamKind::$stream;
                type Params = $params;
                type Result = $result;
                type Event = event_ty!($($event)?);
                $(const SECRET_PARAMS: bool = $secret;)?
                $(
                    fn requires_fd(params: &Self::Params) -> bool {
                        let check: fn(&Self::Params) -> bool = $fd;
                        check(params)
                    }
                )?
            }
        )*

        /// Every method, in declaration order.
        pub const METHODS: &[MethodInfo] = &[$(<$ty as Method>::INFO,)*];

        /// Call `visitor` once per method type.
        pub fn visit_all<V: MethodVisitor>(visitor: &mut V) {
            $(visitor.visit::<$ty>();)*
        }
    };
}

define_methods! {
    // ---- node -----------------------------------------------------------
    /// Liveness probe.
    NodePing { name: "node.ping", role: Viewer, mutating: false, stream: None,
        params: Empty, result: Pong }
    /// The whole-node status report.
    NodeStatus { name: "node.status", role: Viewer, mutating: false, stream: None,
        params: Empty, result: StatusReport }
    /// Replay this node's stranded journal against the shared log.
    NodeReintegrate { name: "node.reintegrate", role: Operator, mutating: true, stream: None,
        params: Empty, result: Ack }
    /// Permanently retire a registry member.
    NodeLeave { name: "node.leave", role: Admin, mutating: true, stream: None,
        params: LeaveParams, result: Ack }
    /// Switch between write-through and write-back.
    NodeSetWriteMode { name: "node.set_write_mode", role: Operator, mutating: true, stream: None,
        params: SetWriteModeParams, result: Ack }
    /// The most recent log lines (optionally following), as chunks. Replaces
    /// the old all-at-once `LogTail`.
    NodeLogsTail { name: "node.logs.tail", role: Viewer, mutating: false, stream: Chunks,
        params: LogTailParams, result: StreamEnd }
    /// Run the backend capability probes.
    NodeDoctor { name: "node.doctor", role: Operator, mutating: false, stream: None,
        params: Empty, result: DoctorStatus }
    /// The operation watchdog's registry (plan 31 §6.10).
    NodeOps { name: "node.ops", role: Viewer, mutating: false, stream: None,
        params: OpsParams, result: OpsReport }
    /// Hand FUSE sessions over (plan 31 §6.11): in place across `exec`
    /// (C4b's upgrade), or to another process over an attached socket fd.
    NodeHandoff { name: "node.handoff", role: Admin, mutating: true, stream: None,
        params: HandoffParams, result: HandoffReport,
        requires_fd: |p| matches!(p.target, HandoffTarget::Socket) }
    /// Inject a host lifecycle event (plan 31 §10, C8): the engine applies
    /// it (a `Suspending` runs its whole sequence) before this answers.
    NodeLifecycle { name: "node.lifecycle", role: Admin, mutating: true, stream: None,
        params: LifecycleParams, result: LifecycleReport }

    // ---- pin ------------------------------------------------------------
    /// Fully cache a subtree and keep it current.
    PinAdd { name: "pin.add", role: Operator, mutating: true, stream: None,
        params: PathParams, result: Ack }
    /// Stop keeping a subtree resident.
    PinRemove { name: "pin.remove", role: Operator, mutating: true, stream: None,
        params: PathParams, result: Ack }
    PinList { name: "pin.list", role: Viewer, mutating: false, stream: None,
        params: Empty, result: PinListing }

    // ---- designation ----------------------------------------------------
    /// Designate this node for a path (DESIGN.md §5.2).
    DesignationOffline { name: "designation.offline", role: Operator, mutating: true, stream: None,
        params: OfflineParams, result: Ack }
    /// Delegate a directory to another node (plan 30 §M11).
    DesignationDelegate { name: "designation.delegate", role: Operator, mutating: true, stream: None,
        params: DelegateParams, result: Ack }
    DesignationUndelegate { name: "designation.undelegate", role: Operator, mutating: true, stream: None,
        params: PathParams, result: Ack }
    DesignationListDelegations { name: "designation.list_delegations", role: Viewer, mutating: false, stream: None,
        params: Empty, result: DelegationListing }
    /// Release this node's designation for a path.
    DesignationOnline { name: "designation.online", role: Operator, mutating: true, stream: None,
        params: PathParams, result: Ack }
    DesignationList { name: "designation.list", role: Viewer, mutating: false, stream: None,
        params: Empty, result: DesignationListing }

    // ---- prune / gc / fsck ----------------------------------------------
    /// Run one retention-prune pass (plan 22).
    PruneRun { name: "prune.run", role: Admin, mutating: true, stream: None,
        params: PruneRunParams, result: Ack }
    PruneList { name: "prune.list", role: Viewer, mutating: false, stream: None,
        params: Empty, result: PruneRootListing }
    /// Chunk and metadata-tree GC, in the daemon's process.
    GcRun { name: "gc.run", role: Admin, mutating: true, stream: None,
        params: GcRunParams, result: GcReport }
    /// `fsck`, in the daemon's process.
    FsckRun { name: "fsck.run", role: Admin, mutating: true, stream: None,
        params: FsckRunParams, result: FsckReport }

    // ---- snapshot / clone -----------------------------------------------
    /// Create a snapshot; `hold` pins it against policy pruning. The record
    /// of the new snapshot comes back.
    SnapshotCreate { name: "snapshot.create", role: Operator, mutating: true, stream: None,
        params: SnapshotCreateParams, result: SnapshotCreated }
    /// (The old `ListSnapshots` alias is gone.)
    SnapshotList { name: "snapshot.list", role: Viewer, mutating: false, stream: None,
        params: SnapshotListParams, result: SnapshotListing }
    SnapshotDelete { name: "snapshot.delete", role: Admin, mutating: true, stream: None,
        params: SnapshotDeleteParams, result: Ack }
    /// Set or release a snapshot's retention hold (plan 32 §0.4). Operator,
    /// like `snapshot.create`: taking a hold protects, it does not destroy.
    /// `force` (overriding another owner's hold) needs admin.
    SnapshotHold { name: "snapshot.hold", role: Operator, mutating: true, stream: None,
        params: SnapshotHoldParams, result: SnapshotHeld }
    SnapshotRefs { name: "snapshot.refs", role: Viewer, mutating: false, stream: None,
        params: SnapRefsParams, result: RefHashes }
    /// Parse a snapshot-schedule expression and report its canonical
    /// form, warnings, steady-state bound and simulated count; with
    /// `against`, evaluate it over that directory's real snapshots as if
    /// it were the directory's policy (plan 32 Step 5). Reads only: an
    /// invalid expression is an `ok: false` result carrying the byte
    /// offset, not a failed call.
    SnapshotPolicyCheck { name: "snapshot.policy.check", role: Viewer, mutating: false, stream: None,
        params: SnapPolicyCheckParams, result: SnapPolicyCheckResult }
    /// Run a policy expression forward from the daemon's clock for
    /// `horizon_ms`, over `path`'s snapshots or none: every snapshot's
    /// fate and the count over time (plan 32 Step 2 "Simulation", the web
    /// UI's retention timeline). An invalid expression is `invalid`.
    SnapshotPolicySimulate { name: "snapshot.policy.simulate", role: Viewer, mutating: false, stream: None,
        params: SnapPolicySimulateParams, result: SnapTimeline }
    /// Every policy root — a directory carrying
    /// `user.constellation.snapshots` — and every orphaned auto-snapshot
    /// stream, with its expression, parse state and auto-snapshot count
    /// (plan 32 Step 3.1). Reads the replica only.
    SnapshotPolicyList { name: "snapshot.policy.list", role: Viewer, mutating: false, stream: None,
        params: Empty, result: SnapPolicyListing }
    /// One directory's policy and, when it parses, its verdict over the
    /// directory's real snapshots. `not_found` for a directory with
    /// neither a policy nor auto snapshots.
    SnapshotPolicyShow { name: "snapshot.policy.show", role: Viewer, mutating: false, stream: None,
        params: PathParams, result: SnapPolicyShown }
    /// Bind a policy to a directory, writing the canonical expression as
    /// the xattr through the same View path a `setxattr` takes (the
    /// validation gate, then a forwarded mutation). Refuses (`conflict`,
    /// the delta in `details`) a change that would expire snapshots unless
    /// `confirm_expiring` equals that count; `dry_run` only reports. A
    /// rename of the directory between evaluation and write is `conflict`.
    /// The guard is this method's: a plain `setxattr` (FUSE, or
    /// `browse.xattr` as an Operator) passes the same validation gate but
    /// asks no confirmation, as plan 32 sanctions — expiry's grace window
    /// (M4) is what protects that path.
    SnapshotPolicySet { name: "snapshot.policy.set", role: Admin, mutating: true, stream: None,
        params: SnapPolicySetParams, result: SnapPolicyDelta }
    /// Unbind a directory's policy. Its auto snapshots become orphaned and
    /// are never deleted automatically; `expire` is refused until expiry
    /// ships (plan 32 M4), and `confirm_expiring` is accepted but ignored
    /// until then.
    SnapshotPolicyRemove { name: "snapshot.policy.remove", role: Admin, mutating: true, stream: None,
        params: SnapPolicyRemoveParams, result: SnapPolicyRoot }
    /// Pause or resume a directory's policy: rewrite its canonical
    /// expression with or without `paused`. Never asks for confirmation —
    /// the retention rule itself does not change.
    SnapshotPolicyPause { name: "snapshot.policy.pause", role: Admin, mutating: true, stream: None,
        params: SnapPolicyPauseParams, result: SnapPolicyRoot }
    /// Clone a snapshot to a destination path.
    CloneCreate { name: "clone.create", role: Operator, mutating: true, stream: None,
        params: CloneParams, result: Ack }

    // ---- browse (ControlVfs) --------------------------------------------
    BrowseReaddir { name: "browse.readdir", role: Viewer, mutating: false, stream: None,
        params: PathParams, result: DirectoryListing }
    BrowseInspect { name: "browse.inspect", role: Viewer, mutating: false, stream: None,
        params: PathParams, result: InspectStatus }
    BrowseStat { name: "browse.stat", role: Viewer, mutating: false, stream: None,
        params: PathParams, result: FileStat }
    /// Read a byte range as chunks.
    BrowseRead { name: "browse.read", role: Viewer, mutating: false, stream: Chunks,
        params: BrowseReadParams, result: StreamEnd }
    BrowseWrite { name: "browse.write", role: Operator, mutating: true, stream: None,
        params: BrowseWriteParams, result: WriteResult }
    BrowseMkdir { name: "browse.mkdir", role: Operator, mutating: true, stream: None,
        params: MkdirParams, result: FileStat }
    BrowseRename { name: "browse.rename", role: Operator, mutating: true, stream: None,
        params: RenameParams, result: Ack }
    BrowseDelete { name: "browse.delete", role: Admin, mutating: true, stream: None,
        params: DeleteParams, result: Ack }
    /// Get/list/set/remove extended attributes. Setting
    /// `user.constellation.snapshots` here meets the View's policy gate
    /// but not `snapshot.policy.set`'s `confirm_expiring` guard (a
    /// `setxattr` never asks; plan 32 relies on expiry's grace window).
    BrowseXattr { name: "browse.xattr", role: Operator, mutating: true, stream: None,
        params: XattrParams, result: XattrResult }

    // ---- locks ----------------------------------------------------------
    /// Voluntarily release a locally held partition lease.
    LocksForceRelease { name: "locks.force_release", role: Admin, mutating: true, stream: None,
        params: ForceReleaseParams, result: Ack }
    /// Discard the journal records held back behind an unrecoverable inode.
    LocksDropHeld { name: "locks.drop_held", role: Admin, mutating: true, stream: None,
        params: DropHeldParams, result: Ack }

    // ---- cache / quota --------------------------------------------------
    CacheList { name: "cache.list", role: Viewer, mutating: false, stream: None,
        params: Empty, result: CacheEntryListing }
    CachePrune { name: "cache.prune", role: Operator, mutating: true, stream: None,
        params: CachePruneParams, result: CachePruneResult }
    /// Set (or clear) the cluster-wide byte cap, or one directory
    /// subtree's (`subtree`); returns the new state.
    QuotaSet { name: "quota.set", role: Admin, mutating: true, stream: None,
        params: SetQuotaParams, result: QuotaStatus }
    QuotaGet { name: "quota.get", role: Viewer, mutating: false, stream: None,
        params: QuotaGetParams, result: QuotaStatus }

    // ---- view -----------------------------------------------------------
    /// Attach a view. With `MountSource::PreopenedFd` the `/dev/fuse` fd
    /// rides on the request.
    ViewMount { name: "view.mount", role: Admin, mutating: true, stream: None,
        params: ViewMountParams, result: ViewInfo,
        requires_fd: |p| matches!(p.source, MountSource::PreopenedFd) }
    ViewUnmount { name: "view.unmount", role: Admin, mutating: true, stream: None,
        params: ViewUnmountParams, result: Ack }
    /// Mounted views, optionally filtered by labels.
    ViewList { name: "view.list", role: Viewer, mutating: false, stream: None,
        params: ViewListParams, result: ViewListing }
    /// One view's statfs / rsize / rcount.
    ViewStats { name: "view.stats", role: Viewer, mutating: false, stream: None,
        params: ViewStatsParams, result: ViewStatsReport }

    // ---- peers / streams ------------------------------------------------
    PeersList { name: "peers.list", role: Viewer, mutating: false, stream: None,
        params: Empty, result: PeerListing }
    /// Periodic counters and gauges.
    StatsSubscribe { name: "stats.subscribe", role: Viewer, mutating: false, stream: Events,
        params: StatsSubscribeParams, result: StreamEnd, event: StatsSample }
    /// Discrete daemon events.
    EventsSubscribe { name: "events.subscribe", role: Viewer, mutating: false, stream: Events,
        params: EventsSubscribeParams, result: StreamEnd, event: ControlEvent }

    // ---- fs registry ----------------------------------------------------
    FsList { name: "fs.list", role: Viewer, mutating: false, stream: None,
        params: Empty, result: FsListing }
    /// Idempotent by `(bucket, prefix)`.
    FsCreate { name: "fs.create", role: Admin, mutating: true, stream: None,
        params: FsCreateParams, result: FsCreated }
    FsImport { name: "fs.import", role: Admin, mutating: true, stream: None,
        params: FsImportParams, result: FsInfo }
    FsExport { name: "fs.export", role: Admin, mutating: false, stream: None,
        params: FsExportParams, result: FsExportDocument }
    FsPasswd { name: "fs.passwd", role: Admin, mutating: true, stream: None,
        params: FsPasswdParams, result: Ack, secret_params: true }
    FsDoctor { name: "fs.doctor", role: Operator, mutating: false, stream: None,
        params: FsDoctorParams, result: FsDoctorReport }
    /// Supply runtime credentials (plan 31 §9.8); memory only.
    FsUnlock { name: "fs.unlock", role: Admin, mutating: true, stream: None,
        params: FsUnlockParams, result: Ack, secret_params: true }
}

/// Look a method up by wire name.
pub fn method_info(name: &str) -> Option<&'static MethodInfo> {
    METHODS.iter().find(|m| m.name == name)
}

/// The 37 variants of the retired `crates/api` `Request` enum and the one
/// control method each became (plan 31 §9.2). `ListSnapshots` was a
/// documented alias of `SnapshotList` and collapses into `snapshot.list`.
/// Kept as data so a test proves the new table loses nothing.
pub const OLD_REQUEST_MAP: [(&str, &str); 37] = [
    ("Ping", "node.ping"),
    ("Status", "node.status"),
    ("Pin", "pin.add"),
    ("Unpin", "pin.remove"),
    ("ListPins", "pin.list"),
    ("Offline", "designation.offline"),
    ("Delegate", "designation.delegate"),
    ("Undelegate", "designation.undelegate"),
    ("ListDelegations", "designation.list_delegations"),
    ("Online", "designation.online"),
    ("ListDesignations", "designation.list"),
    ("Reintegrate", "node.reintegrate"),
    ("Leave", "node.leave"),
    ("SetWriteMode", "node.set_write_mode"),
    ("PruneRun", "prune.run"),
    ("PruneList", "prune.list"),
    ("GcRun", "gc.run"),
    ("FsckRun", "fsck.run"),
    ("SnapshotCreate", "snapshot.create"),
    ("SnapshotList", "snapshot.list"),
    ("ListSnapshots", "snapshot.list"),
    ("SnapshotDelete", "snapshot.delete"),
    ("Clone", "clone.create"),
    ("SnapRefs", "snapshot.refs"),
    ("ReadDir", "browse.readdir"),
    ("Inspect", "browse.inspect"),
    ("ForceRelease", "locks.force_release"),
    ("LogTail", "node.logs.tail"),
    ("Doctor", "node.doctor"),
    ("CacheList", "cache.list"),
    ("CachePrune", "cache.prune"),
    ("SetQuota", "quota.set"),
    ("GetQuota", "quota.get"),
    ("MountAdd", "view.mount"),
    ("MountRemove", "view.unmount"),
    ("MountList", "view.list"),
    ("DropHeld", "locks.drop_held"),
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;
    use std::collections::HashSet;

    #[derive(Default)]
    struct Collect {
        names: Vec<&'static str>,
        infos: Vec<MethodInfo>,
        streaming_shapes_ok: bool,
    }

    impl MethodVisitor for Collect {
        fn visit<M: Method>(&mut self) {
            self.names.push(M::NAME);
            self.infos.push(M::INFO);
            match M::STREAMING {
                StreamKind::None => {
                    self.streaming_shapes_ok &= TypeId::of::<M::Event>() == TypeId::of::<NoEvent>();
                }
                StreamKind::Chunks => {
                    self.streaming_shapes_ok &= TypeId::of::<M::Result>()
                        == TypeId::of::<StreamEnd>()
                        && TypeId::of::<M::Event>() == TypeId::of::<NoEvent>();
                }
                StreamKind::Events => {
                    self.streaming_shapes_ok &= TypeId::of::<M::Result>()
                        == TypeId::of::<StreamEnd>()
                        && TypeId::of::<M::Event>() != TypeId::of::<NoEvent>();
                }
            }
        }
    }

    fn collect() -> Collect {
        let mut c = Collect {
            streaming_shapes_ok: true,
            ..Default::default()
        };
        visit_all(&mut c);
        c
    }

    #[test]
    fn methods_table_matches_every_impl_and_has_no_duplicates() {
        let c = collect();
        assert_eq!(
            c.infos.as_slice(),
            METHODS,
            "METHODS must equal the visited impls in order"
        );
        let unique: HashSet<_> = METHODS.iter().map(|m| m.name).collect();
        assert_eq!(unique.len(), METHODS.len(), "duplicate method names");
        assert_eq!(
            METHODS.len(),
            65,
            "36 old methods + 21 new ones + snapshot.hold + snapshot.policy.check/simulate \
             + snapshot.policy.list/show/set/remove/pause"
        );
        for m in METHODS {
            assert!(
                m.name.split('.').all(|seg| !seg.is_empty()
                    && seg.chars().all(|c| c.is_ascii_lowercase() || c == '_')),
                "{} is not dotted lowercase",
                m.name
            );
            assert_eq!(method_info(m.name), Some(m));
        }
        assert!(method_info("no.such").is_none());
        assert!(
            c.streaming_shapes_ok,
            "streaming methods must return StreamEnd"
        );
    }

    #[test]
    fn every_old_request_variant_maps_to_exactly_one_method() {
        assert_eq!(OLD_REQUEST_MAP.len(), 37);
        let variants: HashSet<_> = OLD_REQUEST_MAP.iter().map(|(v, _)| *v).collect();
        assert_eq!(variants.len(), 37, "a variant is listed twice");
        for (variant, method) in OLD_REQUEST_MAP {
            let hits = METHODS.iter().filter(|m| m.name == method).count();
            assert_eq!(hits, 1, "{variant} -> {method}");
        }
        // 37 variants onto 36 methods: exactly the ListSnapshots collapse.
        let targets: HashSet<_> = OLD_REQUEST_MAP.iter().map(|(_, m)| *m).collect();
        assert_eq!(targets.len(), 36);
        let both: Vec<_> = OLD_REQUEST_MAP
            .iter()
            .filter(|(_, m)| *m == "snapshot.list")
            .map(|(v, _)| *v)
            .collect();
        assert_eq!(both, ["SnapshotList", "ListSnapshots"]);
    }

    #[test]
    fn every_planned_new_method_exists() {
        for name in [
            "fs.list",
            "fs.create",
            "fs.import",
            "fs.export",
            "fs.passwd",
            "fs.doctor",
            "fs.unlock",
            "browse.readdir",
            "browse.inspect",
            "browse.stat",
            "browse.read",
            "browse.write",
            "browse.mkdir",
            "browse.rename",
            "browse.delete",
            "browse.xattr",
            "peers.list",
            "stats.subscribe",
            "events.subscribe",
            "view.stats",
            "node.handoff",
            "node.ops",
            "node.lifecycle",
        ] {
            assert!(method_info(name).is_some(), "{name}");
        }
    }

    #[test]
    fn role_classes_follow_the_documented_table() {
        let role = |n: &str| method_info(n).unwrap().min_role;
        for n in [
            "node.ping",
            "node.status",
            "pin.list",
            "quota.get",
            "browse.read",
            "view.stats",
            "snapshot.policy.check",
            "snapshot.policy.simulate",
            "snapshot.policy.list",
            "snapshot.policy.show",
        ] {
            assert_eq!(role(n), Role::Viewer, "{n}");
            assert!(!method_info(n).unwrap().mutating, "{n}");
        }
        for n in [
            "pin.add",
            "cache.prune",
            "designation.offline",
            "snapshot.create",
        ] {
            assert_eq!(role(n), Role::Operator, "{n}");
        }
        for n in [
            "node.leave",
            "fsck.run",
            "locks.force_release",
            "locks.drop_held",
            "gc.run",
            "prune.run",
            "fs.create",
            "fs.unlock",
            "view.mount",
            "view.unmount",
            "node.handoff",
            "node.lifecycle",
            "quota.set",
            "snapshot.policy.set",
            "snapshot.policy.remove",
            "snapshot.policy.pause",
        ] {
            assert_eq!(role(n), Role::Admin, "{n}");
            assert!(method_info(n).unwrap().mutating, "{n}");
        }
        // Every read-only viewer method is non-mutating; every mutating
        // method needs at least operator.
        for m in METHODS {
            if m.mutating {
                assert!(m.min_role >= Role::Operator, "{}", m.name);
            }
        }
        assert!(
            METHODS
                .iter()
                .filter(|m| m.streaming != StreamKind::None)
                .count()
                >= 4
        );
    }

    #[test]
    fn requires_fd_only_for_preopened_mounts_and_handoff() {
        let path_mount = ViewMountParams {
            subtree: "/".into(),
            source: MountSource::Path {
                mountpoint: "/mnt".into(),
                opts: Default::default(),
            },
            labels: Default::default(),
            qos: Default::default(),
            confine_links: false,
        };
        let fd_mount = ViewMountParams {
            source: MountSource::PreopenedFd,
            ..path_mount.clone()
        };
        assert!(!ViewMount::requires_fd(&path_mount));
        assert!(ViewMount::requires_fd(&fd_mount));
        assert!(!NodeHandoff::requires_fd(&HandoffParams::default()));
        assert!(NodeHandoff::requires_fd(&HandoffParams {
            target: HandoffTarget::Socket,
            ..HandoffParams::default()
        }));
        assert!(!NodePing::requires_fd(&Empty {}));
    }
}
