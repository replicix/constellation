//! The host's lifecycle, applied to an [`crate::Engine`] (plan 31 §10, C8):
//! what the engine does when the host goes to the background, is about to
//! be suspended, resumes, changes network or runs low on power — within
//! the modes its [`EngineProfile`] allows.
//!
//! The events come from the host's `LifecycleSource` (`HostServices::
//! lifecycle`): the OS on a phone (plan 36's `platform::android`), a
//! [`constellation_platform::ManualLifecycle`] on Linux and macOS, which
//! `node.lifecycle` pushes into so the harness can drive the same path. The
//! engine subscribes when it starts; an `engine-lifecycle` thread applies
//! each event in order ([`Lifecycle::apply`]; a suspension parks on view
//! barriers and on the sync task, so it is never a tokio worker's job).
//!
//! ## Effects, by event
//!
//! | event | effect |
//! |---|---|
//! | `Foreground` | foreground; low power ends |
//! | `Background` | background: an `OnDemand` profile pauses its background work |
//! | `LowPower` | as `Background` for an `OnDemand` profile, until the next `Foreground` or `Resumed` |
//! | `NetworkChanged{metered}` | an `UnmeteredOnly` profile holds opportunistic uploads while metered ([`crate::upload::UploadHold`]); iroh re-probes its paths when reachable |
//! | `Suspending{deadline}` | the suspension sequence below, reported in a [`api::SuspendReport`] |
//! | `Resumed` | the resumption below |
//!
//! "Background work" is what a node does on its own schedule rather than
//! for an op: bucket GC, the completed-rid and retention pruners, the
//! cooperative cache's digests, pin refreshes, the placement RTT gossip,
//! the registry/roster and designation polls, read-time atime flushes and
//! the replica's tombstone vacuum. It pauses at the top of its next tick
//! ([`BackgroundGate`]) and resumes where it left off. Never paused: the
//! authority core (lease renewal, tailing, shipping, forwarding), the
//! open-orphan hold writer (an application may hold an orphan open across
//! any of this), and sequential readahead (it runs only behind a reader).
//!
//! ## Suspending
//!
//! 1. Background work pauses.
//! 2. Every open view's `sync_view` barrier, in parallel: each write
//!    session published, the replica synced (and, under `--fsync-mode
//!    s3`, the journal in the log). This runs *before* anything narrows
//!    the node's authority, so a publish that needs the lease can still
//!    take it the ordinary way.
//! 3. The authority core is told it is suspended
//!    (`constellation_authority::AuthorityMode`): from here it takes no
//!    lease — no acquisition, handoff request, `wanted_by` or offer —
//!    except a sealed backup's takeover and a continuation epoch's flush
//!    re-claim, the two acquisitions that exist to keep acknowledged work.
//! 4. The flush (`Control::Flush`, `leave`'s): every pending chunk
//!    uploaded (an explicit durability barrier, so a metered network's
//!    upload hold does not apply), the journal shipped, a commit
//!    published if this node holds the lease, and the lease released —
//!    the clean release a peer takes over from at once, with no TTL to
//!    wait out.
//! 5. P2P quiesces: inbound connections and gossip's dials refused, every open
//!    one closed with a close frame, so a peer's pooled request fails now
//!    rather than at its timeout and nobody waits on this node.
//!
//! Each step is bounded by what is left of the deadline; a step still
//! running when it passes keeps running (nothing is cancelled half-way)
//! and the report says what was not finished. The steps that cost
//! nothing — narrowing the authority, quiescing P2P — always run.
//!
//! ## Resumed
//!
//! P2P admits connections again (inbound only under `Listen`), tells
//! iroh the network may have changed, re-dials every peer and re-forms
//! the gossip neighborhood; the core may take leases again, lazily, on
//! the next mutation that needs one (exactly as a restarted node does);
//! background work resumes; a sync round runs at once so the replica
//! catches up with what the cluster did meanwhile.
//!
//! ## Why no acknowledged write is lost, and no lease is misread
//!
//! - An acknowledgement is only ever given under the filesystem's ack
//!   policy, by the same code as always; nothing here acknowledges
//!   anything. Everything acknowledged before the suspension is in the
//!   local journal (fjall, synced by step 2's barrier) and — once step 4
//!   finished — in the log, which is where any other node reads it.
//! - The lease is only ever let go through the core's ordinary release
//!   CAS (after its journal shipped), or not at all. A flush that could
//!   not finish by the deadline leaves the tenure exactly as a node that
//!   froze would: renewed until it cannot be, then taken over by TTL,
//!   with this node's unshipped journal stranded and replayed by rid on
//!   its return — the deposition path every `kill -9` scenario covers.
//! - While suspended the node is an ordinary non-holder that will not
//!   become a holder: its ops forward (over the connections it opens
//!   itself, or the S3 inbox) to whoever holds the lease, or wait for
//!   one, and end as any non-holder's do. It stays a backup and a promiser for others (both
//!   are how *they* keep acknowledged work, never a claim of its own),
//!   and a backup's seal-and-takeover stays allowed for the same reason.

use crate::authority_driver::CoreStatus;
use crate::profile::{BackgroundMode, EngineProfile, LeaseMode, P2pMode, UploadMode};
use crate::sync::SyncRequest;
use crate::view::View;
use constellation_control::proto::types as api;
use constellation_meta::{Meta, MetaStore};
use constellation_platform::{LifecycleEvent, LifecycleSource};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

/// Whether the node's background work may run (see the module docs).
/// Each ticker waits on it at the top of its next tick.
pub struct BackgroundGate {
    paused: AtomicBool,
    tx: tokio::sync::watch::Sender<bool>,
}

impl BackgroundGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            paused: AtomicBool::new(false),
            tx: tokio::sync::watch::Sender::new(false),
        })
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    pub(crate) fn set(&self, paused: bool) {
        if self.paused.swap(paused, Ordering::Relaxed) != paused {
            tracing::info!(paused, "background work");
        }
        self.tx.send_replace(paused);
    }

    /// Wait while paused; at once when not.
    pub async fn wait_active(&self) {
        if !self.is_paused() {
            return;
        }
        let mut rx = self.tx.subscribe();
        let _ = rx.wait_for(|paused| !*paused).await;
    }
}

/// What the host last said.
#[derive(Debug, Clone, Default)]
struct State {
    background: bool,
    low_power: bool,
    unreachable: bool,
    metered: bool,
    suspending: bool,
    suspended: bool,
    events: u64,
    last_event: Option<String>,
    last_suspend: Option<api::SuspendReport>,
    last_resume: Option<api::ResumeReport>,
}

/// The engine handles a lifecycle event reaches.
pub(crate) struct LifecycleDeps {
    pub sync_tx: tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    pub peers: constellation_net::Peers,
    pub upload: Arc<crate::upload::UploadRuntime>,
    pub write_mode: Arc<crate::writeback::WriteModeState>,
    pub meta: Arc<Meta>,
    pub lease: Arc<crate::lease::LeaseView>,
    pub core_status: Arc<Mutex<CoreStatus>>,
    pub background: Arc<BackgroundGate>,
    pub rt: tokio::runtime::Handle,
}

/// One engine's lifecycle (see the module docs).
pub struct Lifecycle {
    profile: EngineProfile,
    deps: LifecycleDeps,
    views: Mutex<BTreeMap<u64, Weak<View>>>,
    state: Mutex<State>,
    /// Events applied, for a waiter on one it pushed.
    applied: (Mutex<u64>, Condvar),
    /// One event at a time.
    applying: Mutex<()>,
    /// One `node.lifecycle` at a time (its answer is the state its own
    /// event left).
    injecting: Mutex<()>,
}

/// Why [`Lifecycle::inject`] could not deliver an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectRefused {
    /// The host's events come from the OS; none can be injected.
    NotManual,
    /// Nothing subscribes to the source (the engine is shutting down).
    NotListening,
}

impl std::fmt::Display for InjectRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotManual => {
                "this host's lifecycle events come from the OS; none can be injected"
            }
            Self::NotListening => {
                "the engine is not listening for lifecycle events (shutting down?)"
            }
        })
    }
}

/// How long `node.lifecycle` waits for an event other than `Suspending`
/// to be applied, and past a suspension's deadline.
const INJECT_WAIT: Duration = Duration::from_secs(30);

impl Lifecycle {
    pub(crate) fn new(profile: EngineProfile, deps: LifecycleDeps) -> Arc<Self> {
        deps.peers.set_dial_only(profile.p2p == P2pMode::DialOnly);
        let this = Arc::new(Self {
            profile,
            deps,
            views: Mutex::new(BTreeMap::new()),
            state: Mutex::new(State::default()),
            applied: (Mutex::new(0), Condvar::new()),
            applying: Mutex::new(()),
            injecting: Mutex::new(()),
        });
        this.apply_effects();
        this
    }

    /// Apply every event `source` delivers from now on, on a thread of its
    /// own, until `stop` (the engine's shutdown) or the source is gone.
    pub(crate) fn spawn(self: &Arc<Self>, source: &dyn LifecycleSource, stop: Arc<AtomicBool>) {
        let subscription = source.subscribe();
        let this = self.clone();
        let spawned = std::thread::Builder::new()
            .name("engine-lifecycle".into())
            .spawn(move || loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                match subscription.recv_timeout(Duration::from_millis(500)) {
                    Some(event) => this.apply(event),
                    None => continue,
                }
            });
        if let Err(error) = spawned {
            tracing::warn!(%error, "could not start the lifecycle thread; host events are ignored");
        }
    }

    pub(crate) fn register_view(&self, id: u64, view: Weak<View>) {
        self.views.lock().unwrap().insert(id, view);
    }

    pub(crate) fn unregister_view(&self, id: u64) {
        self.views.lock().unwrap().remove(&id);
    }

    pub fn profile(&self) -> &EngineProfile {
        &self.profile
    }

    /// Whether a suspension is in force (from its sequence's start).
    pub fn is_suspended(&self) -> bool {
        let st = self.state.lock().unwrap();
        st.suspended || st.suspending
    }

    /// Events applied so far.
    pub fn applied(&self) -> u64 {
        *self.applied.0.lock().unwrap()
    }

    /// Push `event` into the host's manual source (`node.lifecycle`) and
    /// wait for this engine to apply it. `Ok(false)`: not applied within
    /// the wait (a suspension still running well past its deadline).
    pub fn inject(
        &self,
        source: &dyn LifecycleSource,
        event: LifecycleEvent,
    ) -> Result<bool, InjectRefused> {
        let manual = source.manual().ok_or(InjectRefused::NotManual)?;
        let _one = self.injecting.lock().unwrap();
        let wait = match event {
            LifecycleEvent::Suspending { deadline } => {
                deadline.saturating_duration_since(Instant::now()) + INJECT_WAIT
            }
            _ => INJECT_WAIT,
        };
        let before = self.applied();
        if manual.push(event) == 0 {
            return Err(InjectRefused::NotListening);
        }
        let (count, applied) = &self.applied;
        let guard = count.lock().unwrap();
        let (guard, _) = applied
            .wait_timeout_while(guard, wait, |n| *n <= before)
            .unwrap();
        Ok(*guard > before)
    }

    /// Apply one event (see the module docs). The lifecycle thread calls
    /// this; so may a test.
    pub fn apply(&self, event: LifecycleEvent) {
        let _one = self.applying.lock().unwrap();
        tracing::info!(event = %describe(&event), "host lifecycle event");
        match event {
            LifecycleEvent::Foreground => {
                let mut st = self.state.lock().unwrap();
                st.background = false;
                st.low_power = false;
            }
            LifecycleEvent::Background => self.state.lock().unwrap().background = true,
            LifecycleEvent::LowPower => self.state.lock().unwrap().low_power = true,
            LifecycleEvent::NetworkChanged { reachable, metered } => {
                {
                    let mut st = self.state.lock().unwrap();
                    st.unreachable = !reachable;
                    st.metered = metered;
                }
                if reachable {
                    let peers = self.deps.peers.clone();
                    self.deps
                        .rt
                        .spawn(async move { peers.network_change().await });
                }
            }
            LifecycleEvent::Suspending { deadline } => {
                let report = self.suspend(deadline);
                self.state.lock().unwrap().last_suspend = Some(report);
            }
            LifecycleEvent::Resumed => {
                let report = self.resume();
                let mut st = self.state.lock().unwrap();
                st.low_power = false;
                st.last_resume = Some(report);
            }
        }
        {
            let mut st = self.state.lock().unwrap();
            st.events += 1;
            st.last_event = Some(describe(&event));
        }
        self.apply_effects();
        let (count, applied) = &self.applied;
        *count.lock().unwrap() += 1;
        applied.notify_all();
    }

    /// Background pause and upload hold, from the profile and the state.
    fn apply_effects(&self) {
        let (paused, held) = {
            let st = self.state.lock().unwrap();
            let idle_host = st.background || st.low_power;
            (
                st.suspended
                    || st.suspending
                    || (self.profile.background == BackgroundMode::OnDemand && idle_host),
                self.profile.uploads == UploadMode::UnmeteredOnly && st.metered,
            )
        };
        self.deps.background.set(paused);
        if self.deps.upload.hold.is_held() != held {
            tracing::info!(held, "chunk uploads (metered network)");
        }
        self.deps.upload.hold.set(held);
        self.deps.write_mode.set_upload_hold(held);
    }

    fn set_authority(&self, suspended: bool, within: Duration) -> Result<(), String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.deps
            .sync_tx
            .send(SyncRequest::Authority {
                forward_only: self.profile.leases == LeaseMode::ForwardOnly,
                suspended,
                reply,
            })
            .map_err(|_| "the sync task is not running".to_string())?;
        match self
            .deps
            .rt
            .block_on(async { tokio::time::timeout(within, receive).await })
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("the sync task stopped".into()),
            Err(_) => Err(format!("not answered within {within:?}")),
        }
    }

    fn suspend(&self, deadline: Instant) -> api::SuspendReport {
        let started = Instant::now();
        let deadline_ms = deadline.saturating_duration_since(started).as_millis() as u64;
        let left = || deadline.saturating_duration_since(Instant::now());
        let mut within = true;
        self.state.lock().unwrap().suspending = true;
        self.apply_effects();

        // 2. Every view's barrier, in parallel, within the deadline.
        let views: Vec<(u64, Arc<View>)> = self
            .views
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(id, weak)| weak.upgrade().map(|view| (*id, view)))
            .collect();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<(u64, Result<(), String>)>();
        for (id, view) in &views {
            let (id, view, done) = (*id, view.clone(), done_tx.clone());
            let spawned = std::thread::Builder::new()
                .name("suspend-sync-view".into())
                .spawn(move || {
                    use constellation_vfs::{Blocking, Caller, OpCtx, OpKind, Vfs};
                    let caller = Caller::new(0, 0, None);
                    let result = Blocking::run(|r| {
                        view.sync_view(&OpCtx::new(OpKind::SyncView, &caller), r)
                    })
                    .map_err(|e| format!("{:?}", e.code()));
                    let _ = done.send((id, result));
                });
            if let Err(error) = spawned {
                let _ = done_tx.send((id, Err(format!("no thread for its barrier: {error}"))));
            }
        }
        drop(done_tx);
        let mut views_synced = 0u64;
        let mut view_errors = Vec::new();
        let mut pending: std::collections::BTreeSet<u64> =
            views.iter().map(|(id, _)| *id).collect();
        while !pending.is_empty() {
            match done_rx.recv_timeout(left()) {
                Ok((id, result)) => {
                    pending.remove(&id);
                    match result {
                        Ok(()) => views_synced += 1,
                        Err(e) => view_errors.push(format!("view {id}: sync_view failed: {e}")),
                    }
                }
                Err(_) => break,
            }
        }
        for id in &pending {
            within = false;
            view_errors.push(format!(
                "view {id}: sync_view still running at the deadline"
            ));
        }
        drop(views);

        // 3. No lease from here on (always: it costs nothing).
        if let Err(e) = self.set_authority(true, left().max(Duration::from_secs(2))) {
            within = false;
            tracing::warn!(error = %e, "suspending: narrowing the node's authority failed");
        }

        // 4. The flush: chunks up, journal shipped, lease released.
        let (reply, receive) = tokio::sync::oneshot::channel();
        let (flushed, flush_error) = if self
            .deps
            .sync_tx
            .send(SyncRequest::Flush { reply })
            .is_err()
        {
            (false, Some("the sync task is not running".to_string()))
        } else {
            let budget = left();
            match self
                .deps
                .rt
                .block_on(async { tokio::time::timeout(budget, receive).await })
            {
                Ok(Ok(Ok(()))) => (true, None),
                Ok(Ok(Err(e))) => (false, Some(e)),
                Ok(Err(_)) => (false, Some("the sync task stopped".into())),
                Err(_) => (
                    false,
                    Some("still running at the deadline (it goes on in the background)".into()),
                ),
            }
        };
        within &= flushed;

        // 5. Quiet P2P (always: nobody should wait on a suspended node).
        let peers = self.deps.peers.clone();
        let closed = self.deps.rt.block_on(async move {
            tokio::time::timeout(Duration::from_secs(2), peers.quiesce())
                .await
                .unwrap_or(0)
        }) as u64;

        {
            let mut st = self.state.lock().unwrap();
            st.suspending = false;
            st.suspended = true;
        }
        // The lease mirror follows the core's step that answered the
        // flush; give it that step.
        let mirror_by = Instant::now() + Duration::from_secs(1);
        while flushed && self.deps.lease.status().held && Instant::now() < mirror_by {
            std::thread::sleep(Duration::from_millis(10));
        }
        let elapsed = started.elapsed();
        let report = api::SuspendReport {
            deadline_ms,
            elapsed_ms: elapsed.as_millis() as u64,
            within_deadline: within && Instant::now() <= deadline,
            views: (views_synced + view_errors.len() as u64),
            views_synced,
            view_errors,
            flushed,
            flush_error,
            lease_released: !self.deps.lease.status().held,
            journal_backlog: MetaStore::journal_len(&*self.deps.meta).unwrap_or(u64::MAX),
            pending_uploads: self.deps.meta.pending_upload_count().unwrap_or(u64::MAX),
            p2p_connections_closed: closed,
        };
        if report.within_deadline {
            tracing::info!(?report, "suspended");
        } else {
            tracing::warn!(
                ?report,
                "suspended; not everything finished by the deadline"
            );
        }
        report
    }

    fn resume(&self) -> api::ResumeReport {
        let started = Instant::now();
        let was_suspended = {
            let st = self.state.lock().unwrap();
            st.suspended || st.suspending
        };
        if !was_suspended {
            return api::ResumeReport {
                elapsed_ms: 0,
                was_suspended: false,
                p2p_resumed: false,
            };
        }
        let p2p = self.deps.peers.is_enabled();
        if p2p {
            let peers = self.deps.peers.clone();
            let dial_only = self.profile.p2p == P2pMode::DialOnly;
            // The re-dial is best-effort and bounded: a peer that is
            // gone costs one failed dial, and the registry poll finds
            // the rest.
            self.deps.rt.block_on(async move {
                let _ =
                    tokio::time::timeout(Duration::from_secs(5), peers.unquiesce(dial_only)).await;
            });
        }
        if let Err(e) = self.set_authority(false, Duration::from_secs(10)) {
            tracing::warn!(error = %e, "resuming: restoring the node's authority failed");
        }
        self.state.lock().unwrap().suspended = false;
        let _ = self.deps.sync_tx.send(SyncRequest::Nudge);
        let report = api::ResumeReport {
            elapsed_ms: started.elapsed().as_millis() as u64,
            was_suspended,
            p2p_resumed: p2p,
        };
        tracing::info!(?report, "resumed");
        report
    }

    /// `node.status`'s lifecycle section.
    pub fn status(&self) -> api::LifecycleStatus {
        let st = self.state.lock().unwrap().clone();
        let authority = self.deps.core_status.lock().unwrap().authority;
        let (accepts, gossips) = self.deps.peers.admission();
        let (deferrals, _) = self.deps.upload.hold.counters();
        api::LifecycleStatus {
            profile: [
                ("p2p", self.profile.p2p.as_str()),
                ("leases", self.profile.leases.as_str()),
                ("uploads", self.profile.uploads.as_str()),
                ("background", self.profile.background.as_str()),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            state: if st.suspended {
                "suspended"
            } else if st.suspending {
                "suspending"
            } else if st.background {
                "background"
            } else {
                "foreground"
            }
            .into(),
            low_power: st.low_power,
            network_reachable: !st.unreachable,
            network_metered: st.metered,
            forward_only: authority.forwards(),
            suspended: authority.suspended,
            uploads_held: self.deps.upload.hold.is_held(),
            upload_deferrals: deferrals,
            background_paused: self.deps.background.is_paused(),
            p2p_accepts_inbound: accepts,
            p2p_gossip: gossips,
            events: st.events,
            last_event: st.last_event,
            last_suspend: st.last_suspend,
            last_resume: st.last_resume,
        }
    }
}

/// An event as the status and the log name it.
pub fn describe(event: &LifecycleEvent) -> String {
    match event {
        LifecycleEvent::Foreground => "foreground".into(),
        LifecycleEvent::Background => "background".into(),
        LifecycleEvent::Suspending { deadline } => format!(
            "suspending (deadline in {} ms)",
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
        ),
        LifecycleEvent::Resumed => "resumed".into(),
        LifecycleEvent::NetworkChanged { reachable, metered } => {
            format!("network changed (reachable {reachable}, metered {metered})")
        }
        LifecycleEvent::LowPower => "low power".into(),
    }
}

/// The wire's event (a relative deadline) as the host's.
pub fn event_from_spec(spec: api::LifecycleEventSpec) -> LifecycleEvent {
    match spec {
        api::LifecycleEventSpec::Foreground => LifecycleEvent::Foreground,
        api::LifecycleEventSpec::Background => LifecycleEvent::Background,
        api::LifecycleEventSpec::Suspending { deadline_in_ms } => LifecycleEvent::Suspending {
            deadline: Instant::now() + Duration::from_millis(deadline_in_ms),
        },
        api::LifecycleEventSpec::Resumed => LifecycleEvent::Resumed,
        api::LifecycleEventSpec::NetworkChanged { reachable, metered } => {
            LifecycleEvent::NetworkChanged { reachable, metered }
        }
        api::LifecycleEventSpec::LowPower => LifecycleEvent::LowPower,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EngineConfig, EngineProfile};
    use constellation_platform::HostServices;
    use constellation_store_s3::{ChunkStore, FsMeta};
    use constellation_vfs::{
        Blocking, Caller, Durability, FrontendCaps, Name, OpCtx, OpKind, OpenFlags, OpenOwner, Vfs,
        WriteData,
    };

    /// One engine on a fresh `file://` backend (P2P off: a single node).
    struct Node {
        engine: Arc<crate::Engine>,
        rt: tokio::runtime::Runtime,
        _dir: tempfile::TempDir,
    }

    fn node(profile: EngineProfile) -> Node {
        let dir = tempfile::tempdir().unwrap();
        let backend = format!("file://{}", dir.path().join("backend").display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let store = ChunkStore::new(
            rt.block_on(crate::backend::open_backend(&backend))
                .expect("open backend"),
        );
        rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
            .expect("create_fs");
        let engine = crate::Engine::start(
            EngineConfig {
                state_dir: Some(dir.path().join("state")),
                cache_size: 64 * 1024 * 1024,
                runtime: Some(rt.handle().clone()),
                ..EngineConfig::new(&backend)
            },
            HostServices::native(),
            EngineProfile {
                p2p: P2pMode::Off,
                ..profile
            },
        )
        .expect("Engine::start");
        Node {
            engine: Arc::new(engine),
            rt,
            _dir: dir,
        }
    }

    impl Drop for Node {
        fn drop(&mut self) {
            let _ = self.engine.shutdown();
        }
    }

    fn cx<'a>(kind: OpKind, caller: &'a Caller) -> OpCtx<'a> {
        OpCtx::new(kind, caller)
    }

    /// Create `name` in the view's root, write `data`, `fsync` it: its
    /// acknowledgement.
    fn write_file(view: &View, name: &str, data: &[u8]) -> constellation_vfs::VfsResult<()> {
        let caller = Caller::new(0, 0, None);
        let root = view.view_root();
        let (entry, opened) = Blocking::run(|r| {
            view.create(
                &cx(OpKind::Create, &caller),
                root,
                Name::new(name),
                0o100644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })?;
        let ino = entry.attr.ino;
        Blocking::run(|r| {
            view.write(
                &cx(OpKind::Write, &caller),
                ino,
                opened.fh,
                0,
                WriteData::Borrowed(data),
                OpenFlags::WRITE,
                r,
            )
        })?;
        Blocking::run(|r| {
            view.fsync(
                &cx(OpKind::Fsync, &caller),
                ino,
                opened.fh,
                Durability::Configured,
                r,
            )
        })?;
        Blocking::run(|r| {
            view.release(
                &cx(OpKind::Release, &caller),
                ino,
                opened.fh,
                OpenFlags::WRITE,
                None,
                r,
            )
        })
    }

    fn open_view(node: &Node) -> Arc<View> {
        node.engine
            .open_view(
                crate::ViewSpec::new("/"),
                FrontendCaps::linux_fuse(false),
                crate::DeferredEvents::new(),
            )
            .expect("open_view")
    }

    fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
        let until = Instant::now() + Duration::from_secs(20);
        while !ok() {
            assert!(Instant::now() < until, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The whole suspend/resume state machine on one node: a suspension
    /// publishes the view's pending write, ships the journal, releases the
    /// lease and narrows the core's authority, all inside the deadline; the
    /// node still reads while suspended; a resume restores everything and
    /// the next write takes the lease again, lazily.
    #[test]
    fn suspend_publishes_ships_and_releases_then_resume_restores() {
        let node = node(EngineProfile::desktop());
        let view = open_view(&node);
        write_file(&view, "before", b"acknowledged before the suspension").unwrap();
        let lifecycle = node.engine.lifecycle().clone();
        wait_for("the lease", || node.engine.lease().status().held);

        lifecycle.apply(LifecycleEvent::Suspending {
            deadline: Instant::now() + Duration::from_secs(15),
        });
        let status = lifecycle.status();
        let report = status.last_suspend.clone().expect("a suspend report");
        assert!(report.within_deadline, "{report:?}");
        assert!(report.flushed, "{report:?}");
        assert!(report.lease_released, "{report:?}");
        assert_eq!((report.views, report.views_synced), (1, 1), "{report:?}");
        assert_eq!((report.journal_backlog, report.pending_uploads), (0, 0));
        assert_eq!(status.state, "suspended");
        assert!(status.background_paused);
        assert!(!node.engine.lease().status().held);
        wait_for("the core's suspended mode", || {
            let mode = node.engine.core_status().lock().unwrap().authority;
            mode.suspended && mode.forwards()
        });
        assert!(lifecycle.status().suspended && lifecycle.status().forward_only);
        // Local reads keep working while suspended.
        let caller = Caller::new(0, 0, None);
        let entry = Blocking::run(|r| {
            view.lookup(
                &cx(OpKind::Lookup, &caller),
                view.view_root(),
                Name::new("before"),
                r,
            )
        })
        .unwrap();
        assert_eq!(entry.attr.size, 34);

        lifecycle.apply(LifecycleEvent::Resumed);
        let status = lifecycle.status();
        assert_eq!(status.state, "foreground");
        assert!(status.last_resume.as_ref().unwrap().was_suspended);
        assert!(!status.background_paused);
        wait_for("the core's hold mode", || {
            let mode = node.engine.core_status().lock().unwrap().authority;
            !mode.suspended && !mode.forwards()
        });
        // Lazily, as a restarted node: the next write takes the lease.
        write_file(&view, "after", b"after the resume").unwrap();
        assert!(node.engine.lease().status().held);
        assert_eq!(lifecycle.status().events, 2);
        node.engine.close_view(&view);
        drop(node.rt.handle().clone());
    }

    /// `Background`/`LowPower` pause an `OnDemand` profile's background
    /// work until `Foreground`; a `Continuous` one pauses only while
    /// suspended.
    #[test]
    fn background_work_follows_the_background_mode() {
        let on_demand = node(EngineProfile {
            background: BackgroundMode::OnDemand,
            ..EngineProfile::desktop()
        });
        let l = on_demand.engine.lifecycle().clone();
        assert!(!l.status().background_paused);
        l.apply(LifecycleEvent::Background);
        assert!(l.status().background_paused);
        assert_eq!(l.status().state, "background");
        l.apply(LifecycleEvent::Foreground);
        assert!(!l.status().background_paused);
        l.apply(LifecycleEvent::LowPower);
        assert!(l.status().background_paused && l.status().low_power);
        l.apply(LifecycleEvent::Foreground);
        assert!(!l.status().background_paused && !l.status().low_power);
        drop(on_demand);

        let continuous = node(EngineProfile::desktop());
        let l = continuous.engine.lifecycle().clone();
        l.apply(LifecycleEvent::Background);
        l.apply(LifecycleEvent::LowPower);
        assert!(!l.status().background_paused, "continuous work runs on");
        l.apply(LifecycleEvent::Suspending {
            deadline: Instant::now() + Duration::from_secs(10),
        });
        assert!(l.status().background_paused, "…except while suspended");
        l.apply(LifecycleEvent::Resumed);
        assert!(!l.status().background_paused);
    }

    /// `UnmeteredOnly` holds uploads exactly while the network is metered;
    /// `Always` never does.
    #[test]
    fn uploads_hold_on_a_metered_network_only_when_unmetered_only() {
        let node = node(EngineProfile {
            uploads: UploadMode::UnmeteredOnly,
            ..EngineProfile::desktop()
        });
        let l = node.engine.lifecycle().clone();
        let metered = |metered| LifecycleEvent::NetworkChanged {
            reachable: true,
            metered,
        };
        l.apply(metered(true));
        assert!(l.status().uploads_held && l.status().network_metered);
        assert!(node.engine.upload().hold.is_held());
        assert_eq!(
            node.engine.write_mode().effective(false, false, false),
            crate::writeback::WriteMode::Back,
            "plain closes do not wait for an upload the hold would not start"
        );
        assert_eq!(
            node.engine.write_mode().effective(true, false, false),
            crate::writeback::WriteMode::Through,
            "an fsync still uploads"
        );
        l.apply(metered(false));
        assert!(!l.status().uploads_held);
        assert!(!node.engine.upload().hold.is_held());
        drop(node);

        let always = self::node(EngineProfile::desktop());
        let l = always.engine.lifecycle().clone();
        l.apply(metered(true));
        assert!(!l.status().uploads_held && l.status().network_metered);
    }

    /// A metered hold keeps a plain close's chunk off S3 (it stays
    /// pending, locally durable) and an fsync'd file's chunk goes up; the
    /// hold's end drains the rest.
    #[test]
    fn a_held_background_pass_uploads_nothing_but_an_fsync_does() {
        let node = node(EngineProfile {
            uploads: UploadMode::UnmeteredOnly,
            ..EngineProfile::desktop()
        });
        let view = open_view(&node);
        let l = node.engine.lifecycle().clone();
        l.apply(LifecycleEvent::NetworkChanged {
            reachable: true,
            metered: true,
        });
        // A plain close (no fsync): write-back under the hold.
        let caller = Caller::new(0, 0, None);
        let root = view.view_root();
        let (entry, opened) = Blocking::run(|r| {
            view.create(
                &cx(OpKind::Create, &caller),
                root,
                Name::new("plain"),
                0o100644,
                OpenFlags::READ | OpenFlags::WRITE,
                OpenOwner::NONE,
                r,
            )
        })
        .unwrap();
        let data = vec![7u8; 64 * 1024];
        Blocking::run(|r| {
            view.write(
                &cx(OpKind::Write, &caller),
                entry.attr.ino,
                opened.fh,
                0,
                WriteData::Borrowed(&data),
                OpenFlags::WRITE,
                r,
            )
        })
        .unwrap();
        Blocking::run(|r| {
            view.release(
                &cx(OpKind::Release, &caller),
                entry.attr.ino,
                opened.fh,
                OpenFlags::WRITE,
                None,
                r,
            )
        })
        .unwrap();
        // Several rounds go by; the chunk stays pending.
        std::thread::sleep(Duration::from_millis(2_000));
        let meta = node.engine.meta().clone();
        assert!(meta.pending_upload_count().unwrap() >= 1, "held");
        assert!(
            node.engine.upload().hold.counters().0 >= 1,
            "deferrals counted"
        );
        // An fsync'd file uploads at once, hold or not.
        write_file(&view, "synced", &vec![9u8; 64 * 1024]).unwrap();
        let synced = meta
            .pending_uploads()
            .unwrap()
            .into_iter()
            .filter(|(_, ino)| *ino != entry.attr.ino)
            .count();
        assert_eq!(synced, 0, "the fsync'd file's chunk is up");
        // Unmetered: the plain close's chunk drains.
        l.apply(LifecycleEvent::NetworkChanged {
            reachable: true,
            metered: false,
        });
        wait_for("the held chunk's upload", || {
            meta.pending_upload_count().unwrap() == 0
        });
        node.engine.close_view(&view);
    }

    /// `node.lifecycle`'s path: an event pushed into the host's manual
    /// source is applied by the engine's own lifecycle thread before
    /// `inject` returns.
    #[test]
    fn an_injected_event_is_applied_by_the_lifecycle_thread() {
        let node = node(EngineProfile::desktop());
        let l = node.engine.lifecycle().clone();
        let host = node.engine.host().clone();
        assert_eq!(
            l.inject(&*host.lifecycle, LifecycleEvent::Background),
            Ok(true)
        );
        assert_eq!(l.status().state, "background");
        assert_eq!(l.status().last_event.as_deref(), Some("background"));
        assert_eq!(
            l.inject(
                &*host.lifecycle,
                LifecycleEvent::Suspending {
                    deadline: Instant::now() + Duration::from_secs(5)
                }
            ),
            Ok(true)
        );
        assert_eq!(l.status().state, "suspended");
        assert_eq!(
            l.inject(&*host.lifecycle, LifecycleEvent::Resumed),
            Ok(true)
        );
        assert_eq!(l.status().events, 3);
    }

    /// `event_from_spec` keeps the relative deadline.
    #[test]
    fn a_wire_deadline_is_relative_to_its_arrival() {
        let before = Instant::now();
        match event_from_spec(api::LifecycleEventSpec::Suspending {
            deadline_in_ms: 1500,
        }) {
            LifecycleEvent::Suspending { deadline } => {
                let d = deadline.duration_since(before);
                assert!(d >= Duration::from_millis(1500) && d < Duration::from_secs(3));
            }
            other => panic!("{other:?}"),
        }
    }
}

/// Plan 31 C8 across nodes: the authority core's forward-only and
/// suspended modes against a shared bucket, through the standalone driver
/// (the same core the daemon runs, IO inline).
#[cfg(test)]
mod authority_tests {
    use crate::authority_driver::Standalone;
    use constellation_meta::Meta;
    use constellation_store_s3::{LeaseMode, LeaseStore};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::Arc;

    fn node(store: &Arc<InMemory>, id: u64) -> (Arc<Meta>, Standalone) {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let driver = Standalone::new(
            meta.clone(),
            store.clone() as Arc<dyn ObjectStore>,
            id,
            LeaseMode::Cas,
        );
        (meta, driver)
    }

    async fn lease(store: &Arc<InMemory>) -> constellation_store_s3::Lease {
        LeaseStore::new(
            store.clone() as Arc<dyn ObjectStore>,
            constellation_store_s3::log::PARTITION,
            LeaseMode::Cas,
        )
        .get()
        .await
        .unwrap()
        .expect("a lease object")
        .0
    }

    fn mkdir(meta: &Meta, name: &str) -> constellation_meta::MutateOp {
        let parent = constellation_fs_core::types::ROOT_INO;
        constellation_meta::MutateOp::Mkdir {
            parent,
            name: name.into(),
            ino: meta.allocate_ino(parent).unwrap(),
            mode: 0o755,
            uid: 0,
            gid: 0,
        }
    }

    fn rid(node: u64, seq: u64) -> constellation_meta::Rid {
        constellation_meta::Rid {
            node,
            incarnation: 1,
            seq,
        }
    }

    /// A forward-only node never asks a live holder for the lease — no
    /// `wanted_by` registration, which is what would make the holder hand
    /// it over — while a holding node does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_only_never_registers_for_a_live_lease() {
        let store = Arc::new(InMemory::new());
        let (_m1, mut holder) = node(&store, 1);
        let (_m2, mut phone) = node(&store, 2);
        let (_m3, mut desktop) = node(&store, 3);
        assert!(holder.acquire().await.unwrap());
        phone.set_authority(true, false).await.unwrap();
        assert!(!phone.acquire().await.unwrap(), "busy");
        // Run whatever S3 the acquisition left queued (a registration
        // would be one) before looking.
        phone.tail_to_head().await.unwrap();
        assert!(
            !lease(&store).await.wanted_by.contains(&2),
            "a forward-only node registered for the lease"
        );
        assert!(!desktop.acquire().await.unwrap(), "busy");
        desktop.tail_to_head().await.unwrap();
        assert!(
            lease(&store).await.wanted_by.contains(&3),
            "a holding node registers (the control)"
        );
    }

    /// With nobody holding, a forward-only node takes the lease rather
    /// than leave the cluster without a sequencer; a suspended one takes
    /// nothing until it resumes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_free_lease_is_claimable_forward_only_but_not_suspended() {
        let store = Arc::new(InMemory::new());
        let (_m1, mut sleeper) = node(&store, 1);
        sleeper.set_authority(false, true).await.unwrap();
        assert!(!sleeper.acquire().await.unwrap(), "suspended: no lease");
        sleeper.set_authority(true, false).await.unwrap();
        assert!(
            sleeper.acquire().await.unwrap(),
            "forward-only: a free lease"
        );
        assert!(sleeper.core().authority_mode().forward_only);
    }

    /// The suspension's flush: the holder's acknowledged journal ships and
    /// its lease is released cleanly, so another node takes over at once
    /// (no TTL) and reads every acknowledged op; the suspended node takes
    /// nothing back meanwhile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_suspension_flush_hands_every_acknowledged_op_to_the_next_holder() {
        use constellation_meta::MetaStore;
        let store = Arc::new(InMemory::new());
        let (m1, mut a) = node(&store, 1);
        let (m2, mut b) = node(&store, 2);
        assert!(a.acquire().await.unwrap());
        for seq in 1..=5 {
            let reply = a
                .submit(rid(1, seq), mkdir(&m1, &format!("d{seq}")))
                .await
                .unwrap();
            assert!(
                matches!(
                    reply,
                    constellation_authority::ClientReply::Outcome(
                        constellation_meta::MutateOutcome::Accepted { .. }
                    )
                ),
                "{reply:?}"
            );
        }
        assert!(
            MetaStore::journal_len(&*m1).unwrap() > 0,
            "unshipped journal"
        );
        a.set_authority(false, true).await.unwrap();
        a.flush_release().await.unwrap();
        assert_eq!(MetaStore::journal_len(&*m1).unwrap(), 0, "shipped");
        let released = lease(&store).await;
        assert!(
            released.released,
            "released, not left to expire: {released:?}"
        );
        assert!(!a.acquire().await.unwrap(), "suspended: takes nothing back");
        assert!(b.acquire().await.unwrap(), "the next holder, at once");
        b.tail_to_head().await.unwrap();
        for seq in 1..=5 {
            assert!(
                m2.lookup(constellation_fs_core::types::ROOT_INO, &format!("d{seq}"))
                    .unwrap()
                    .is_some(),
                "d{seq} lost across the suspension"
            );
        }
    }
}
