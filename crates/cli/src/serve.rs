//! `constellation serve`: a node with no FUSE mount of its own — plan 37's
//! engine pods (§4, §"Engine-pod lifecycle").
//!
//! An engine pod is unprivileged: it can never call `mount(2)`, so the
//! `mount` command's "become the daemon by mounting a first view" shape does
//! not fit it. `serve` starts the same node (`NodeRuntime`), binds the
//! control socket at an explicit path (a hostPath the node plugin reaches),
//! and serves the control API until a signal; views arrive later, if at
//! all, as `view.mount{PreopenedFd}` from the privileged node plugin (K3),
//! and the node keeps running when the last of them goes
//! ([`NodeConfig::persistent`](crate::node_runtime::NodeConfig)). The
//! controller-owned engine pod never gets a view: it exists only to answer
//! `fs.create`/`browse.*`/`quota.*`.
//!
//! `--create` makes the filesystem at `--s3` first when there is none (the
//! controller-owned pod of a pool nobody has provisioned from yet — its
//! daemon cannot start on a filesystem that does not exist, and the
//! controller's own `fs.create` needs a daemon to go through). It is the
//! `fs create` sequence without the registry: an engine pod names its
//! filesystem by URL and state dir, never by a registered name. A
//! filesystem already there is used as it is; the controller's `fs.create`
//! then compares its parameters with the class's and refuses a mismatch.
//!
//! **`--await-unlock`: credentials over the control socket only** (plan 37
//! §9, K6a). An engine pod's S3 credentials and E2E passphrase must never
//! be in its pod spec, environment, image or a hostPath file, so they can
//! only arrive over the control socket — but a daemon cannot start without
//! them (it reads `meta.json` and replays the log first). With the flag,
//! `serve` binds the control socket *before* it starts the node and
//! answers there as a **credential gate**: `node.ping` (the pod's
//! readiness probe passes, so its plugin knows it can talk) and `fs.unlock`
//! naming this engine's `--s3` URL, under the same allowlist and into the
//! same audit log as the daemon proper; anything else is `Unavailable`
//! ("waiting for fs.unlock"). Nothing is read from the environment
//! (`AWS_ACCESS_KEY_ID`, `CONSTELLATION_PASSPHRASE`): the gate checks the
//! credentials it was given against the bucket (reading `meta.json`, or
//! making the filesystem under `--create`) and an E2E filesystem's
//! passphrase against its keyring, refuses ones that do not work and keeps
//! waiting, and otherwise stops accepting, starts the node on a
//! `CredentialSource::Static` over an `EphemeralSecretStore` holding them
//! (passphrase included), hands the same listening socket to the daemon —
//! a client connecting meanwhile waits in its backlog — and only then
//! answers the `fs.unlock` that started it. The gate's connections are
//! closed two seconds later; its caller dials again for everything else.
//! A later `fs.unlock` reaches the running daemon and rotates the
//! credentials in place (`engine::control::fs`), with no remount. An
//! `fs.unlock` with only a passphrase leaves the engine on the
//! environment's AWS chain (IRSA, EKS Pod Identity) — the
//! `aws-default-chain` StorageClass of an E2E pool. While it waits,
//! `SIGTERM`/`SIGINT` end it at once with status 0 (the handlers are
//! installed before the socket exists: as its container's PID 1 it would
//! otherwise ignore them for the pod's whole grace period); an engine that
//! fails to start answers the `fs.unlock` that started it before it exits.
//! The process runs with core dumps off: its credentials live in memory.
//!
//! No fork, no `daemon.lock` attach: a pod runs exactly one node per state
//! dir, and a second `serve` on the same one fails rather than attaching —
//! unless it has a `--handoff-socket` (plan 37 §8): then it waits there as
//! the standby of the pod it replaces (`crate::handoff_socket`), and serves
//! once that pod's sessions are handed over and it has exited. `daemon
//! --upgrade` does not apply.
//!
//! **A standby with `--await-unlock`** has no credential gate: its control
//! socket is bound only once it serves. Its credentials come from the pod
//! it replaces, in the handoff's `Credentials` step, which runs before any
//! session stops (`crate::handoff_socket`) — the node plugin may hold none
//! to send, having restarted with the chart upgrade that rolls the pods.
//! They are checked against the bucket as the gate checks an `fs.unlock`,
//! its pre-open waits for them, and it keeps them for its own successor.
//! Both waits — the gate's and the standby's — end on `SIGTERM`/`SIGINT`
//! (a sealed standby's only at the seal's deadline, `crate::handoff_socket`:
//! the sender may commit at any moment, and then it holds the only copies
//! of the sessions). The handlers are installed once, before either wait,
//! and handed on to the node (`NodeConfig::signals`), so a signal between
//! the wait's end and the node's start is not lost; one a sealed standby
//! received before adopting is passed to the node once its views are
//! resumed (`NodeRuntime::deliver_signal`).
//!
//! `control-relay` (hidden) is the other half of how the CSI controller
//! reaches a controller-owned engine pod: an exec'd process that pipes its
//! stdin/stdout to the control socket, so the controller speaks the control
//! protocol over the Kubernetes exec stream (authorized by RBAC on
//! `pods/exec`) and the daemon sees an ordinary local peer — the pod's own
//! uid, its owner. `--ping` is the pod's readiness probe: one `node.ping`.

use anyhow::{bail, Context, Result};
use constellation_control::methods::{FsUnlock, NodePing};
use constellation_control::proto::types::{Ack, FsUnlockParams, Pong};
use constellation_control::proto::ControlError;
use constellation_control::transport::{Listener, Transport, UnixSocketListener};
use constellation_platform::{CredentialSource, EphemeralSecretStore, SecretStore};
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta, StoreError};
use futures::future::BoxFuture;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroize::Zeroizing;

use crate::node_runtime::StopSignals;
use crate::{log_buffer, node_runtime, parallelism, startup};

/// `constellation serve`'s arguments.
pub struct ServeArgs {
    pub s3: String,
    pub state_dir: PathBuf,
    pub control_socket: PathBuf,
    pub create: bool,
    pub chunk_size: u32,
    pub compression: String,
    pub e2e: bool,
    pub cache_size: Option<u64>,
    pub write_mode: Option<String>,
    /// Wait for `fs.unlock` (module docs).
    pub await_unlock: bool,
    /// Plan 37 §8: where to wait as a handoff standby when another
    /// process holds the state dir (`crate::handoff_socket`).
    pub handoff_socket: Option<PathBuf>,
}

/// The name an unlocked E2E passphrase is held under (the engine's
/// `fs.unlock` uses the same, so a rotation replaces it in place; a
/// socket handoff's `Credentials` step reads it, `crate::handoff_socket`).
pub(crate) const E2E_PASSPHRASE: &str = "e2e_passphrase";

/// How long the gate's own connections outlive the engine's start, so the
/// answer to the `fs.unlock` that started it is flushed before they close.
const GATE_LINGER: Duration = Duration::from_secs(2);

pub fn cmd_serve(
    threads: parallelism::ThreadPlan,
    args: ServeArgs,
    log_buffer: log_buffer::LogBuffer,
) -> Result<()> {
    let ServeArgs {
        s3,
        state_dir,
        control_socket,
        create,
        chunk_size,
        compression,
        e2e,
        cache_size,
        write_mode,
        await_unlock,
        handoff_socket,
    } = args;
    let initial_write_mode: crate::writeback::WriteMode = write_mode
        .as_deref()
        .unwrap_or("through")
        .parse()
        .map_err(anyhow::Error::msg)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.tokio)
        .max_blocking_threads(threads.blocking)
        .enable_all()
        .build()?;
    if let Some(parent) = control_socket.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    startup::phase("taking daemon.lock");
    let standby_on = match (crate::take_state_dir_lock(&state_dir)?, &handoff_socket) {
        (crate::LockOutcome::BecomeDaemon, _) => {
            crate::handoff_socket::clear_stale_marker(&state_dir);
            crate::handoff_socket::clear_stale_standby_marker(&state_dir);
            None
        }
        (crate::LockOutcome::Attach, None) => bail!(
            "another daemon already serves {}; one `serve` per state dir",
            state_dir.display()
        ),
        // Plan 37 §8: the pod this one replaces still serves; wait for its
        // sessions below.
        (crate::LockOutcome::Attach, Some(socket)) => Some(socket.clone()),
    };
    let creation = Creation {
        create,
        chunk_size,
        compression: compression.clone(),
        e2e,
    };
    if await_unlock {
        // Its credentials will live in this process's memory only — a
        // standby's too, from its `Credentials` step on.
        constellation_platform::forbid_core_dumps().context("disabling core dumps")?;
    }
    // Before any socket exists: the engine is its container's PID 1, which
    // the kernel spares every signal it has no handler for, so a pod
    // deleted while it waits (for `fs.unlock`, or as a standby) would
    // otherwise sit out its whole grace period. Handed on to the node,
    // whose own handling (§7) takes over: a signal that came in between is
    // still there for it.
    let mut stop = match await_unlock || standby_on.is_some() {
        true => Some(rt.block_on(async { StopSignals::install() })?),
        false => None,
    };
    let mut adoption = None;
    let mut preopened = None;
    let (credentials, passphrase, gate, secrets) = if let Some(socket) = standby_on {
        let signals = stop.take().expect("installed for a standby");
        match wait_as_standby(&rt, &socket, &state_dir, &s3, await_unlock, signals)? {
            Standing::Adopt {
                adopted,
                unlocked,
                preopen,
                signals,
            } => {
                stop = Some(signals);
                adoption = Some(adopted);
                preopened = preopen.take();
                match unlocked {
                    Some((credentials, secrets)) => (
                        credentials,
                        passphrase_from(secrets.clone()),
                        None,
                        Some(secrets),
                    ),
                    None => (
                        CredentialSource::AwsDefaultChain,
                        // An engine pod has no terminal: an E2E
                        // filesystem's passphrase comes from the
                        // environment or not at all.
                        constellation_engine::PassphraseSource::Ask(Box::new(|| {
                            crate::passphrase("CONSTELLATION_PASSPHRASE", "")
                        })),
                        None,
                        None,
                    ),
                }
            }
            Standing::GiveUp => {
                rt.shutdown_timeout(Duration::from_secs(5));
                return Ok(());
            }
        }
    } else if await_unlock {
        let stop = stop.as_mut().expect("installed for the gate");
        startup::phase("waiting for fs.unlock");
        let mut gate = Gate::open(&rt, &control_socket, &state_dir, &s3)?;
        tracing::info!(socket = %control_socket.display(), s3,
            "waiting for fs.unlock to supply the credentials");
        loop {
            let next = rt.block_on(async {
                tokio::select! {
                    unlock = gate.next() => Ok(unlock),
                    signal = stop.recv() => Err(signal),
                }
            });
            let unlock = match next {
                Ok(Some(unlock)) => unlock,
                Ok(None) => bail!("the credential gate stopped"),
                Err(signal) => {
                    tracing::info!(signal, "stopped while waiting for fs.unlock");
                    gate.close(&rt);
                    return Ok(());
                }
            };
            let Unlock {
                credentials,
                secrets,
                reply,
            } = unlock;
            let checked = rt.block_on(async {
                tokio::select! {
                    checked = check_unlocked(&s3, &creation, &credentials, &secrets) => Ok(checked),
                    signal = stop.recv() => Err(signal),
                }
            });
            match checked {
                Ok(Ok(())) => {
                    gate.accepted = Some(reply);
                    break (
                        credentials,
                        passphrase_from(secrets.clone()),
                        Some(gate),
                        Some(secrets),
                    );
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e.message,
                        "fs.unlock refused: the credentials do not open the filesystem");
                    let _ = reply.send(Err(e));
                }
                Err(signal) => {
                    tracing::info!(signal, "stopped while checking an fs.unlock");
                    let _ = reply.send(Err(ControlError::unavailable(
                        "the engine is stopping; connect again",
                    )));
                    gate.close(&rt);
                    return Ok(());
                }
            }
        }
    } else {
        if create {
            startup::phase("creating the filesystem if missing");
            rt.block_on(create_if_missing(&s3, &creation, None, None))?;
        }
        (
            CredentialSource::AwsDefaultChain,
            // An engine pod has no terminal: an E2E filesystem's
            // passphrase comes from the environment or not at all.
            constellation_engine::PassphraseSource::Ask(Box::new(|| {
                crate::passphrase("CONSTELLATION_PASSPHRASE", "")
            })),
            None,
            None,
        )
    };
    if let Some(gate) = &gate {
        // From here on connections wait in the backlog for the daemon.
        gate.stop_accepting();
    }
    startup::phase("starting the node runtime");
    let node = node_runtime::NodeRuntime::start(
        node_runtime::NodeConfig {
            fs_id: constellation_engine::FsId::new(state_dir.display().to_string()),
            engine: constellation_engine::EngineConfig {
                state_dir: Some(state_dir.clone()),
                cache_size: cache_size.unwrap_or(1024 * 1024 * 1024),
                cto_strict: crate::cto::strict_from(None)?,
                locks: crate::locks::cluster_flag(None)?,
                initial_write_mode,
                atime_mode: crate::atime::AtimeMode::resolve(None),
                passphrase,
                credentials,
                version: env!("CONSTELLATION_VERSION").to_string(),
                preopened,
                ..constellation_engine::EngineConfig::new(s3.clone())
            },
            web_ui: 0,
            log_buffer,
            resumed: adoption.as_ref().map(|a| node_runtime::Resumed {
                generation: a.generation(),
                control: None,
            }),
            // A headless node makes no mount of its own, but `view.mount`
            // with a path (control.rs) makes a plain one — CSI engine pods
            // included — and it follows the same transport policy as a
            // daemon's plain mount (plan 38 Z2c): `auto` unless the
            // environment or the profile says otherwise, cluster-lock
            // mounts included. `view.mount` on a pre-opened descriptor is
            // handover-capable and pinned to /dev/fuse whatever this says.
            fuse_transport: constellation_frontend_fuse::TransportConfig::resolve(
                None,
                None,
                crate::node_runtime::profile_transport()?,
            )
            .map_err(anyhow::Error::msg)?,
            control_socket: Some(control_socket.clone()),
            persistent: true,
            signals: stop,
        },
        rt.handle().clone(),
    );
    let node = match (node, adoption) {
        (Ok(node), Some(adoption)) => {
            // The handed-off views first: their requests have been queued
            // in the kernel since the sender stopped reading.
            startup::phase("resuming the handed-off views");
            let signal = adoption.signal();
            let served = crate::handoff_socket::resume(&node, adoption);
            tracing::info!(served, "handoff complete: serving");
            if let Some(signal) = signal {
                // Only now: with its views mounted, the node defers a
                // `SIGTERM` (§7) rather than exiting under them.
                tracing::info!(signal, "handling the signal received while sealed");
                node.deliver_signal(signal);
            }
            node
        }
        (Ok(node), None) => node,
        (Err(e), adoption) => {
            if let Some(adoption) = adoption {
                crate::handoff_socket::failed(adoption, format!("starting the node: {e:#}"));
            }
            if let Some(gate) = gate {
                gate.answer(
                    &rt,
                    Err(ControlError::failed(format!("starting the engine: {e:#}"))),
                )
                .flush(&rt);
            }
            return Err(e);
        }
    };
    if let Some(secrets) = secrets {
        // What its own successor's `Credentials` step hands over.
        node.keep_handoff_secrets(secrets);
    }
    startup::phase("starting the control API");
    let gate = match gate {
        Some(mut gate) => {
            node.adopt_control_listener(gate.take_listener()?);
            Some(gate)
        }
        None => None,
    };
    if let Err(e) = node.serve_headless() {
        if let Some(gate) = gate {
            gate.answer(
                &rt,
                Err(ControlError::failed(format!(
                    "serving the control API: {e:#}"
                ))),
            )
            .flush(&rt);
        }
        let _ = node.shutdown();
        return Err(e);
    }
    if let Some(gate) = gate {
        let uuid = node.engine().fsmeta().uuid.to_string();
        // The linger runs on the runtime the node keeps.
        let _ = gate.answer(
            &rt,
            Ok(Ack::new(format!(
                "{s3}: credentials accepted; the engine serves filesystem {uuid}"
            ))),
        );
    }
    startup::done("serving (headless)");
    tracing::info!(
        socket = %control_socket.display(),
        fs = %node.engine().fsmeta().uuid,
        s3,
        "serving the control API with no FUSE mount"
    );
    node.wait_stopped();
    let failed = node.shutdown_error();
    drop(node);
    rt.shutdown_timeout(Duration::from_secs(10));
    match failed {
        Some(message) => bail!(message),
        None => Ok(()),
    }
}

/// How a standby's wait ended ([`wait_as_standby`]).
enum Standing {
    /// The state dir is ours: start on `unlocked` (an `--await-unlock`
    /// standby's handed-over credentials, the source and its store) and
    /// resume `adopted`.
    Adopt {
        adopted: crate::handoff_socket::Adoption,
        unlocked: Option<(CredentialSource, EphemeralSecretStore)>,
        preopen: Preopen,
        /// The handlers, for the node (`NodeConfig::signals`).
        signals: StopSignals,
    },
    /// Aborted, timed out or stopped: exit 0, so the pod ends `Succeeded`
    /// instead of being restarted into another standby (its node plugin
    /// deletes it).
    GiveUp,
}

/// Wait as a handoff standby on `socket` (`crate::handoff_socket`) until a
/// sealed handoff hands this process the state dir. `stop` ends the wait
/// as `crate::handoff_socket::standby` says, and is handed back with the
/// adoption. With `await_unlock` its credentials come from
/// the handoff's `Credentials` step only (module docs), and so does its
/// pre-open; without, it pre-opens at once on the environment's.
fn wait_as_standby(
    rt: &tokio::runtime::Runtime,
    socket: &Path,
    state_dir: &Path,
    s3: &str,
    await_unlock: bool,
    mut stop: StopSignals,
) -> Result<Standing> {
    let (stopped, stop_seen) = tokio::sync::watch::channel(None);
    let (done, mut wait_over) = tokio::sync::oneshot::channel::<()>();
    // Holds the handlers until the wait is over, then hands them back
    // (`recv` is cancel-safe: a signal not yet taken stays for the node).
    // A second signal changes nothing: the first one already decided.
    let watcher = rt.spawn(async move {
        tokio::select! {
            signal = stop.recv() => {
                let _ = stopped.send(Some(signal));
                let _ = wait_over.await;
            }
            _ = &mut wait_over => {}
        }
        stop
    });
    let held: Arc<HeldCredentials> = Arc::new(Mutex::new((None, Preopen(None))));
    let hook = if await_unlock {
        let (held, s3, handle) = (held.clone(), s3.to_string(), rt.handle().clone());
        let hook: crate::handoff_socket::CredentialsHook =
            Box::new(move |credentials| standby_credentials(&handle, &s3, &held, credentials));
        Some(hook)
    } else {
        held.lock().unwrap().1 = preopen(s3, CredentialSource::AwsDefaultChain, rt.handle());
        None
    };
    let outcome = crate::handoff_socket::standby(rt.handle(), socket, state_dir, hook, stop_seen);
    let _ = done.send(());
    let signals = rt
        .block_on(watcher)
        .context("the standby's signal watcher panicked")?;
    match outcome? {
        crate::handoff_socket::StandbyOutcome::Adopt(adopted) => {
            let (unlocked, preopen) =
                std::mem::replace(&mut *held.lock().unwrap(), (None, Preopen(None)));
            if await_unlock && unlocked.is_none() {
                // `Receive` refuses until the credentials are in.
                crate::handoff_socket::failed(adopted, "no credentials were handed over".into());
                bail!("a standby awaiting its credentials was sealed without them");
            }
            Ok(Standing::Adopt {
                adopted,
                unlocked,
                preopen,
                signals,
            })
        }
        crate::handoff_socket::StandbyOutcome::GiveUp(why) => {
            tracing::warn!(reason = %why, "the handoff did not happen; exiting");
            // An `Abort`'s answer is still being written.
            std::thread::sleep(Duration::from_millis(300));
            Ok(Standing::GiveUp)
        }
    }
}

/// A standby's handed-over credentials (the source and its store) and the
/// pre-open started on them.
type HeldCredentials = Mutex<(Option<(CredentialSource, EphemeralSecretStore)>, Preopen)>;

/// An `--await-unlock` standby's `Credentials` step (module docs of
/// `crate::handoff_socket`): checked against the bucket as the gate checks
/// an `fs.unlock` (a refused pair fails the step, before any session
/// stopped), then held, and the pre-open started on them. A later step
/// replaces what the held store holds, as one generation.
fn standby_credentials(
    rt: &tokio::runtime::Handle,
    s3: &str,
    held: &HeldCredentials,
    credentials: constellation_control::proto::types::UnlockCredentials,
) -> Result<String, ControlError> {
    let params = FsUnlockParams {
        fs: s3.to_string(),
        credentials,
    };
    let (source, store) = unlock_source(&params)?;
    drop(params);
    // Never `--create`: the sender's filesystem exists.
    let creation = Creation {
        create: false,
        chunk_size: 0,
        compression: String::new(),
        e2e: false,
    };
    rt.block_on(check_unlocked(s3, &creation, &source, &store))?;
    let mut held = held.lock().unwrap();
    match &held.0 {
        Some((have, have_store)) => {
            if matches!(have, CredentialSource::Static(_))
                != matches!(source, CredentialSource::Static(_))
            {
                return Err(ControlError::invalid(
                    "the handed-over credentials changed kind (a key pair, or a passphrase only)",
                ));
            }
            let (_, old) = have_store.snapshot();
            let (_, new) = store.snapshot();
            let mut entries: Vec<(&str, Option<&[u8]>)> = old
                .keys()
                .filter(|k| !new.contains_key(*k))
                .map(|k| (k.as_str(), None))
                .collect();
            entries.extend(new.iter().map(|(k, v)| (k.as_str(), Some(v.expose()))));
            have_store
                .replace(&entries)
                .map_err(|e| ControlError::failed(e.to_string()))?;
            Ok("credentials checked against the bucket; the held ones replaced".into())
        }
        None => {
            let for_preopen = match &source {
                CredentialSource::Static(store) => CredentialSource::Static(store.clone()),
                _ => CredentialSource::AwsDefaultChain,
            };
            held.1 = preopen(s3, for_preopen, rt);
            held.0 = Some((source, store));
            tracing::info!("handed-over credentials accepted; pre-opening with them");
            Ok("credentials checked against the bucket; pre-opening with them".into())
        }
    }
}

/// Plan 37 §8: a standby's [`constellation_engine::Engine::preopen`],
/// started on a thread of its own (at once, or for an `--await-unlock`
/// standby once its credentials are in), so the backend client, its
/// probe and the P2P endpoint are ready when the sender commits — the
/// engine start that follows is the pause its sessions' callers see.
struct Preopen(Option<std::thread::JoinHandle<Option<constellation_engine::Preopened>>>);

impl Preopen {
    /// What it opened, once it has (it usually has long since: the
    /// standby waits for the plugin's whole prepare and transfer). Bounded:
    /// past [`PREOPEN_WAIT`] the start opens everything itself.
    fn take(mut self) -> Option<constellation_engine::Preopened> {
        let handle = self.0.take()?;
        let deadline = std::time::Instant::now() + PREOPEN_WAIT;
        while !handle.is_finished() {
            if std::time::Instant::now() >= deadline {
                tracing::warn!("the pre-open is still running; the start opens everything itself");
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        handle.join().ok().flatten()
    }
}

/// How long a sealed standby waits for its pre-open to finish.
const PREOPEN_WAIT: Duration = Duration::from_secs(10);

fn preopen(s3: &str, credentials: CredentialSource, rt: &tokio::runtime::Handle) -> Preopen {
    let (s3, rt) = (s3.to_string(), rt.clone());
    let spawned = std::thread::Builder::new()
        .name("handoff-preopen".into())
        .spawn(move || {
            let started = std::time::Instant::now();
            // As `NodeRuntime::start` picks it.
            let p2p = constellation_engine::EngineProfile::from_env(
                constellation_engine::EngineProfile::desktop(),
            )
            .map(|profile| profile.p2p_enabled())
            .unwrap_or(true);
            match constellation_engine::Engine::preopen(
                &s3,
                credentials,
                &constellation_platform::HostServices::native(),
                p2p,
                &rt,
            ) {
                Ok(preopened) => {
                    tracing::info!(
                        ms = started.elapsed().as_millis() as u64,
                        "pre-opened the backend and the P2P endpoint for the handoff"
                    );
                    Some(preopened)
                }
                Err(e) => {
                    tracing::warn!(error = %format!("{e:#}"),
                        "pre-opening for the handoff failed; the start opens everything itself");
                    None
                }
            }
        });
    Preopen(spawned.ok())
}

/// `--create` and the filesystem flags that go with it.
struct Creation {
    create: bool,
    chunk_size: u32,
    compression: String,
    e2e: bool,
}

/// `fs create`'s backend half (module docs): a no-op when `meta.json`
/// exists, a create otherwise. A concurrent creator winning the race is
/// success too — both then serve the one filesystem. `credentials` and
/// `passphrase`: what `fs.unlock` gave the gate (`None`: the environment).
async fn create_if_missing(
    s3: &str,
    c: &Creation,
    credentials: Option<&Arc<CredentialSource>>,
    passphrase: Option<Zeroizing<String>>,
) -> Result<()> {
    constellation_fs_core::validate_chunk_size(c.chunk_size)?;
    let setting: CompressionSetting = c.compression.parse().map_err(|e| anyhow::anyhow!("{e}"))?;
    let (backend, _) = crate::backend::open_backend_described_with(s3, credentials)
        .await
        .context("opening backend")?;
    let store = ChunkStore::new(backend);
    match store.load_fs_waiting(Duration::from_secs(2)).await {
        Ok(meta) => {
            tracing::info!(fs = %meta.uuid, s3, "filesystem exists; serving it");
            return Ok(());
        }
        Err(StoreError::NotFound) => {}
        Err(e) => return Err(e).context("reading meta.json"),
    }
    crate::preflight_backend(&store, s3).await?;
    let mut meta = FsMeta::new(c.chunk_size, &setting.to_string());
    meta.e2e = c.e2e;
    if c.e2e {
        meta.gossip_secret = None;
        let secret = match passphrase {
            Some(p) => p,
            None if credentials.is_some() => {
                bail!("an e2e filesystem needs the passphrase in fs.unlock (e2e_passphrase)")
            }
            None => crate::passphrase("CONSTELLATION_PASSPHRASE", "")
                .context("an e2e filesystem needs CONSTELLATION_PASSPHRASE")?,
        };
        meta.keyring = Some(
            constellation_store_s3::create_keyring_block(&secret)
                .context("creating E2E keyring")?,
        );
    }
    match store.create_fs(&meta).await {
        Ok(()) => tracing::info!(fs = %meta.uuid, s3, "created the filesystem"),
        Err(StoreError::AlreadyExists) => tracing::info!(s3, "another creator won; serving theirs"),
        Err(e) => return Err(e).context("creating filesystem"),
    }
    Ok(())
}

/// One `fs.unlock` the gate received, waiting for its answer.
struct Unlock {
    credentials: CredentialSource,
    /// Everything it carried: the key pair (the very store a `Static`
    /// source signs from) and the passphrase.
    secrets: EphemeralSecretStore,
    reply: tokio::sync::oneshot::Sender<Result<Ack, ControlError>>,
}

/// `fs.unlock`'s credentials as a source and a store (module docs): a
/// key pair makes a `Static` source over the store; a passphrase alone
/// leaves the AWS chain.
fn unlock_source(
    p: &FsUnlockParams,
) -> Result<(CredentialSource, EphemeralSecretStore), ControlError> {
    let c = &p.credentials;
    let has_keys = c.access_key_id.is_some() || c.secret_access_key.is_some();
    if has_keys && (c.access_key_id.is_none() || c.secret_access_key.is_none()) {
        return Err(ControlError::invalid(
            "access_key_id and secret_access_key go together",
        ));
    }
    if !has_keys && c.e2e_passphrase.is_none() {
        return Err(ControlError::invalid("no credentials to unlock"));
    }
    let store = EphemeralSecretStore::new();
    let mut entries: Vec<(&str, Option<&[u8]>)> = Vec::new();
    if let (Some(id), Some(secret)) = (&c.access_key_id, &c.secret_access_key) {
        entries.push((
            CredentialSource::ACCESS_KEY_ID,
            Some(id.expose().as_bytes()),
        ));
        entries.push((
            CredentialSource::SECRET_ACCESS_KEY,
            Some(secret.expose().as_bytes()),
        ));
        if let Some(token) = &c.session_token {
            entries.push((
                CredentialSource::SESSION_TOKEN,
                Some(token.expose().as_bytes()),
            ));
        }
    }
    if let Some(passphrase) = &c.e2e_passphrase {
        entries.push((E2E_PASSPHRASE, Some(passphrase.expose().as_bytes())));
    }
    store
        .replace(&entries)
        .map_err(|e| ControlError::failed(e.to_string()))?;
    let source = if has_keys {
        CredentialSource::Static(store.clone())
    } else {
        CredentialSource::AwsDefaultChain
    };
    Ok((source, store))
}

/// The engine's passphrase source over the unlocked store.
fn passphrase_from(store: EphemeralSecretStore) -> constellation_engine::PassphraseSource {
    constellation_engine::PassphraseSource::Ask(Box::new(move || {
        let secret = store
            .get(E2E_PASSPHRASE)?
            .context("an e2e filesystem needs the passphrase in fs.unlock (e2e_passphrase)")?;
        let text = secret
            .expose_str()
            .context("the e2e passphrase is not UTF-8")?
            .to_string();
        if text.is_empty() {
            bail!("the e2e passphrase must not be empty");
        }
        Ok(Zeroizing::new(text))
    }))
}

/// Whether the unlocked credentials open the filesystem at `s3` (or, with
/// `--create`, make it), and an E2E filesystem's passphrase its keyring:
/// checked before the gate gives up its socket, so a wrong key or
/// passphrase is an `fs.unlock` error and the gate keeps waiting, never a
/// crash-looping pod.
async fn check_unlocked(
    s3: &str,
    creation: &Creation,
    credentials: &CredentialSource,
    secrets: &EphemeralSecretStore,
) -> Result<(), ControlError> {
    let source = Arc::new(match credentials {
        CredentialSource::Static(store) => CredentialSource::Static(store.clone()),
        _ => CredentialSource::AwsDefaultChain,
    });
    let passphrase = secrets
        .get(E2E_PASSPHRASE)
        .ok()
        .flatten()
        .and_then(|s| s.expose_str().map(|t| Zeroizing::new(t.to_string())));
    let checked = async {
        if creation.create {
            create_if_missing(s3, creation, Some(&source), passphrase.clone()).await?;
        }
        let (backend, _) = crate::backend::open_backend_described_with(s3, Some(&source))
            .await
            .context("opening backend")?;
        let meta = ChunkStore::new(backend)
            .load_fs_waiting(Duration::from_secs(2))
            .await
            .with_context(|| format!("reading meta.json at {s3}"))?;
        if meta.e2e {
            let Some(passphrase) = &passphrase else {
                return Err(WrongSecret(
                    "the filesystem is end-to-end encrypted, and fs.unlock carried no \
                     e2e_passphrase"
                        .into(),
                )
                .into());
            };
            // The engine unlocks it again at start; a wrong one fails here.
            if meta.unlock(passphrase).is_err() {
                return Err(WrongSecret(
                    "the e2e_passphrase does not unlock the filesystem's keyring".into(),
                )
                .into());
            }
        }
        anyhow::Ok(())
    };
    checked.await.map_err(|e| gate_refusal(&e))
}

/// A secret `fs.unlock` carried that the filesystem rejects.
#[derive(Debug)]
struct WrongSecret(String);

impl std::fmt::Display for WrongSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WrongSecret {}

/// The `fs.unlock` error for a failed gate check, classified as the
/// running engine's rotation probe does: a wrong passphrase or a
/// permanent S3 refusal (a pair S3 rejects) is `Denied`, which the plugins
/// do not retry; anything transient is `Unavailable`, which they do.
fn gate_refusal(e: &anyhow::Error) -> ControlError {
    let message = format!("{e:#}");
    let permanent = e.downcast_ref::<WrongSecret>().is_some()
        || !constellation_store_s3::classify_chain(e.chain()).is_transient();
    match permanent {
        true => ControlError::new(constellation_control::proto::ErrorKind::Denied, message),
        false => ControlError::unavailable(message),
    }
}

/// The credential gate (module docs).
struct Gate {
    unlocks: tokio::sync::mpsc::Receiver<Unlock>,
    stop: tokio::sync::watch::Sender<bool>,
    server: Option<constellation_control::server::ServerHandle>,
    /// The listening socket the daemon gets.
    listener: Option<std::os::unix::net::UnixListener>,
    /// The answer of the `fs.unlock` whose credentials the engine starts
    /// with.
    accepted: Option<tokio::sync::oneshot::Sender<Result<Ack, ControlError>>>,
}

impl Gate {
    fn open(
        rt: &tokio::runtime::Runtime,
        socket: &Path,
        state_dir: &Path,
        s3: &str,
    ) -> Result<Gate> {
        let _guard = rt.enter();
        let bound = UnixSocketListener::bind(socket)
            .with_context(|| format!("binding control socket {}", socket.display()))?;
        let listener = bound.try_clone_std()?;
        let canonical = std::fs::canonicalize(socket).unwrap_or_else(|_| socket.to_path_buf());
        let (tx, unlocks) = tokio::sync::mpsc::channel::<Unlock>(4);
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let mut router = constellation_control::Router::new().with_unregistered_error(
            ControlError::unavailable(constellation_control::proto::AWAITING_UNLOCK),
        );
        router.register::<NodePing, _, _>(|_, _| async { Ok(Pong {}) });
        let own = s3.to_string();
        router.register::<FsUnlock, _, _>(move |_, p: FsUnlockParams| {
            let tx = tx.clone();
            let own = own.clone();
            async move {
                if p.fs != own {
                    return Err(ControlError::invalid(format!(
                        "this engine serves {own}; fs.unlock names {:?}",
                        p.fs
                    )));
                }
                let (credentials, secrets) = unlock_source(&p)?;
                let (reply, answer) = tokio::sync::oneshot::channel();
                let gone = || {
                    ControlError::unavailable(
                        "the engine started from another fs.unlock meanwhile; connect again",
                    )
                };
                tx.send(Unlock {
                    credentials,
                    secrets,
                    reply,
                })
                .await
                .map_err(|_| gone())?;
                answer.await.map_err(|_| gone())?
            }
        });
        let router = crate::control::with_daemon_policy(router, state_dir, Some(canonical));
        let server = constellation_control::server::serve_router(
            GateListener {
                inner: Some(bound.keep_path_on_drop()),
                stop: stopped,
            },
            Arc::new(router),
        );
        Ok(Gate {
            unlocks,
            stop,
            server: Some(server),
            listener: Some(listener),
            accepted: None,
        })
    }

    async fn next(&mut self) -> Option<Unlock> {
        self.unlocks.recv().await
    }

    fn stop_accepting(&self) {
        let _ = self.stop.send(true);
    }

    fn take_listener(&mut self) -> Result<std::os::unix::net::UnixListener> {
        self.listener
            .take()
            .context("the credential gate's listener is gone")
    }

    /// Answer the accepted `fs.unlock`, and close the gate's connections a
    /// moment later (module docs). A caller about to exit waits for that
    /// ([`Closing::flush`]), or the answer may never be written.
    fn answer(
        mut self,
        rt: &tokio::runtime::Runtime,
        result: Result<Ack, ControlError>,
    ) -> Closing {
        if let Some(reply) = self.accepted.take() {
            let _ = reply.send(result);
        }
        // Queued unlocks are answered by dropping them ("connect again").
        self.unlocks.close();
        Closing(self.server.take().map(|server| {
            rt.spawn(async move {
                tokio::time::sleep(GATE_LINGER).await;
                server.shutdown().await;
            })
        }))
    }

    /// Stop serving at once (the process is stopping): queued and
    /// in-flight unlocks are answered "connect again".
    fn close(mut self, rt: &tokio::runtime::Runtime) {
        self.stop_accepting();
        self.unlocks.close();
        if let Some(server) = self.server.take() {
            rt.block_on(server.shutdown());
        }
    }
}

/// The gate's lingering shutdown after its answer.
#[must_use = "an exiting caller must flush the answer"]
struct Closing(Option<tokio::task::JoinHandle<()>>);

impl Closing {
    /// Wait until the gate's connections are closed: its answer has been
    /// written by then.
    fn flush(self, rt: &tokio::runtime::Runtime) {
        if let Some(task) = self.0 {
            let _ = rt.block_on(task);
        }
    }
}

/// The gate's listener: a clone of the daemon's listening socket that
/// stops accepting (and closes its descriptor, never the socket file)
/// once told to, so the daemon is the only acceptor from then on.
struct GateListener {
    inner: Option<UnixSocketListener>,
    stop: tokio::sync::watch::Receiver<bool>,
}

impl Listener for GateListener {
    fn accept(&mut self) -> BoxFuture<'_, std::io::Result<Arc<dyn Transport>>> {
        Box::pin(async move {
            let GateListener { inner, stop } = self;
            loop {
                if *stop.borrow() {
                    *inner = None;
                }
                let Some(listener) = inner.as_mut() else {
                    return std::future::pending().await;
                };
                tokio::select! {
                    changed = stop.changed() => {
                        if changed.is_err() {
                            *inner = None;
                        }
                    }
                    accepted = listener.accept() => return accepted,
                }
            }
        })
    }
}

/// `control-relay`: pipe stdin → `socket` and `socket` → stdout until
/// either side closes; with `ping`, one `node.ping` instead (on `socket`,
/// else on `or_socket`).
pub async fn control_relay(socket: &Path, ping: bool, or_socket: Option<&Path>) -> Result<()> {
    if ping {
        let ping = |socket: &Path| {
            let socket = socket.to_path_buf();
            async move {
                let client = constellation_control::Client::connect_unix(&socket).await?;
                client
                    .call_bounded::<constellation_control::methods::NodePing>(
                        Default::default(),
                        Duration::from_secs(2),
                    )
                    .await?;
                anyhow::Ok(())
            }
        };
        return match (ping(socket).await, or_socket) {
            (Ok(()), _) => Ok(()),
            (Err(e), None) => Err(e),
            (Err(e), Some(other)) => ping(other)
                .await
                .with_context(|| format!("and {}: {e:#}", socket.display())),
        };
    }
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connecting to {}", socket.display()))?;
    let (mut from_daemon, mut to_daemon) = stream.into_split();
    let up = async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = stdin.read(&mut buf).await?;
            if n == 0 {
                // The caller is done; let the daemon see EOF and finish.
                return to_daemon.shutdown().await;
            }
            to_daemon.write_all(&buf[..n]).await?;
        }
    };
    let down = async move {
        let mut stdout = tokio::io::stdout();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = from_daemon.read(&mut buf).await?;
            if n == 0 {
                return std::io::Result::Ok(());
            }
            // Frames must reach the caller as they come, not when a
            // buffer fills: the protocol is request/response.
            stdout.write_all(&buf[..n]).await?;
            stdout.flush().await?;
        }
    };
    tokio::pin!(down);
    tokio::select! {
        result = &mut down => result?,
        result = up => {
            result?;
            down.await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_control::proto::ErrorKind;

    #[test]
    fn the_gate_refuses_a_bad_secret_for_good_and_a_flaky_s3_for_now() {
        let wrong = anyhow::Error::new(WrongSecret("the e2e_passphrase is wrong".into()));
        assert_eq!(gate_refusal(&wrong).kind, ErrorKind::Denied);
        let forbidden = anyhow::anyhow!("HTTP status 403 Forbidden, code SignatureDoesNotMatch")
            .context("reading meta.json");
        assert_eq!(gate_refusal(&forbidden).kind, ErrorKind::Denied);
        let busy = anyhow::anyhow!("HTTP status 503 Service Unavailable, code SlowDown")
            .context("reading meta.json");
        assert_eq!(gate_refusal(&busy).kind, ErrorKind::Unavailable);
    }
}
