//! Constellation entry point: CLI, daemon, and FUSE mount in one binary.

mod backend;
mod fusefs;
mod lease;
mod prefetch;
mod shipper;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::SqliteMeta;
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta, StoreError};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "constellation",
    version,
    about = "Distributed POSIX filesystem on S3"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Filesystem lifecycle.
    Fs {
        #[command(subcommand)]
        command: FsCommand,
    },
    /// Mount a filesystem.
    Mount {
        /// Backend: s3://bucket/prefix, file:///path, or absolute path.
        #[arg(long)]
        s3: String,
        /// Mountpoint directory.
        mountpoint: PathBuf,
        /// Local state directory (metadata DB + chunk cache).
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Chunk cache budget in bytes.
        #[arg(long, default_value_t = 10 * 1024 * 1024 * 1024)]
        cache_size: u64,
        /// Allow other users to access the mount.
        #[arg(long)]
        allow_other: bool,
        /// What fsync() waits for: "local" (journal on disk; background
        /// ship) or "s3" (record durable in the shared log).
        #[arg(long, default_value = "local")]
        fsync_mode: String,
    },
    /// Verify backend capabilities (conditional writes, filesystem state).
    Doctor {
        #[arg(long)]
        s3: String,
    },
    /// Show filesystem information from the backend, or live daemon
    /// status (spool backlog, cache) from a mount's state dir.
    Status {
        #[arg(long, conflicts_with = "state_dir")]
        s3: Option<String>,
        /// State dir of a running mount: query its control socket.
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum FsCommand {
    /// Create a new filesystem at an empty prefix.
    Create {
        #[arg(long)]
        s3: String,
        /// Chunk size in bytes (power of two, 1-64 MiB).
        #[arg(long, default_value_t = constellation_fs_core::DEFAULT_CHUNK_SIZE)]
        chunk_size: u32,
        /// Root compression setting (raw, zstd, zstd:LEVEL).
        #[arg(long, default_value = "zstd:3")]
        compression: String,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    match cli.command {
        Command::Fs {
            command:
                FsCommand::Create {
                    s3,
                    chunk_size,
                    compression,
                },
        } => {
            constellation_fs_core::validate_chunk_size(chunk_size)?;
            let setting: CompressionSetting =
                compression.parse().map_err(|e| anyhow::anyhow!("{e}"))?;
            let store = ChunkStore::new(backend::open_backend(&s3)?);
            let meta = FsMeta::new(chunk_size, &setting.to_string());
            rt.block_on(store.create_fs(&meta))
                .context("creating filesystem")?;
            println!("created filesystem {} at {s3}", meta.uuid);
            println!("  chunk_size:  {chunk_size}");
            println!("  compression: {setting}");
            Ok(())
        }
        Command::Doctor { s3 } => {
            let store = ChunkStore::new(backend::open_backend(&s3)?);
            let caps = rt.block_on(store.probe_conditional_writes())?;
            let yn = |b: bool| if b { "ok" } else { "MISSING" };
            println!(
                "create-if-absent (If-None-Match) ... {}",
                yn(caps.create_if_absent)
            );
            println!("etag CAS (If-Match) ............... {}", yn(caps.etag_cas));
            if !caps.create_if_absent {
                bail!("backend lacks create-if-absent; unusable as a constellation backend");
            }
            if !caps.etag_cas {
                println!(
                    "note: etag CAS unavailable; single-node mounts work (leases degrade to \
                     create-only, single-writer assumed), but lease renew/takeover — and \
                     therefore multi-node mounts — need If-Match"
                );
            }
            print!("filesystem at prefix .............. ");
            match rt.block_on(store.load_fs()) {
                Ok(meta) => println!("ok ({}, format v{})", meta.uuid, meta.format_version),
                Err(StoreError::NotFound) => println!("none (run `constellation fs create`)"),
                Err(e) => bail!(e),
            }
            Ok(())
        }
        Command::Status { s3, state_dir } => match (s3, state_dir) {
            (Some(s3), None) => {
                let store = ChunkStore::new(backend::open_backend(&s3)?);
                let meta = rt.block_on(store.load_fs())?;
                println!("{}", serde_json::to_string_pretty(&meta)?);
                Ok(())
            }
            (None, Some(dir)) => {
                let sock = dir.join(constellation_api::SOCKET_NAME);
                let resp = rt.block_on(constellation_api::call(
                    &sock,
                    &constellation_api::Request::Status,
                ))?;
                match resp {
                    constellation_api::Response::Status(s) => {
                        println!("{}", serde_json::to_string_pretty(&s)?)
                    }
                    other => bail!("unexpected response: {other:?}"),
                }
                Ok(())
            }
            _ => bail!("exactly one of --s3 or --state-dir is required"),
        },
        Command::Mount {
            s3,
            mountpoint,
            state_dir,
            cache_size,
            allow_other,
            fsync_mode,
        } => {
            let fsync_s3 = match fsync_mode.as_str() {
                "local" => false,
                "s3" => true,
                other => bail!("invalid --fsync-mode {other:?} (expected local or s3)"),
            };
            mount(
                rt,
                &s3,
                &mountpoint,
                state_dir,
                cache_size,
                allow_other,
                fsync_s3,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn mount(
    rt: tokio::runtime::Runtime,
    s3: &str,
    mountpoint: &std::path::Path,
    state_dir: Option<PathBuf>,
    cache_size: u64,
    allow_other: bool,
    fsync_s3: bool,
) -> Result<()> {
    let store = std::sync::Arc::new(ChunkStore::new(backend::open_backend(s3)?));
    let fsmeta = rt
        .block_on(store.load_fs())
        .context("loading filesystem (fs create first?)")?;
    let state_dir = state_dir.unwrap_or_else(|| default_state_dir(&fsmeta));
    std::fs::create_dir_all(&state_dir)?;
    let log = constellation_store_s3::LogStore::new(store.inner().clone());
    let db_path = state_dir.join("meta.db");
    // Fresh node: rebuild the replica from checkpoint + log replay.
    if !db_path.exists() {
        rt.block_on(shipper::bootstrap(&db_path, &log))
            .context("bootstrapping metadata replica")?;
    }
    let meta = std::sync::Arc::new(SqliteMeta::open(&db_path)?);
    // Node identity: claim a cluster-unique id on first mount of this
    // state dir; it scopes ino allocation and marks log segment origin.
    let node_id: u64 = match meta.kv_get("node_id")? {
        Some(v) => v.parse().context("corrupt node_id in state dir")?,
        None => {
            let id = rt
                .block_on(constellation_store_s3::claim_node_id(store.inner().clone()))
                .context("claiming node id")?;
            meta.kv_set("node_id", &id.to_string())?;
            id
        }
    };
    meta.set_node_prefix(node_id)?;
    tracing::info!(node_id, "node identity");
    let cache = std::sync::Arc::new(DiskCache::open(state_dir.join("cache"), cache_size)?);
    let compression: CompressionSetting = fsmeta
        .compression
        .parse()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Write authority (DESIGN.md §4/§5). Renew and takeover need
    // If-Match; a backend without it can only be driven safely by one
    // node at a time, so say so loudly and fall back to create-only
    // lease semantics instead of refusing to mount at all.
    let caps = rt
        .block_on(store.probe_conditional_writes())
        .context("probing backend conditional writes")?;
    if !caps.create_if_absent {
        bail!(
            "backend lacks create-if-absent (If-None-Match); unusable as a constellation backend"
        );
    }
    let lease_mode = if caps.etag_cas {
        constellation_store_s3::LeaseMode::Cas
    } else {
        tracing::warn!(
            "backend has no etag CAS (If-Match): lease renew/takeover cannot be \
             enforced. Running in single-writer mode — mount this filesystem from \
             ONE node only. Run `constellation doctor` and use an S3 backend with \
             If-Match for multi-node operation."
        );
        constellation_store_s3::LeaseMode::SingleWriter
    };
    let keeper = lease::LeaseKeeper::new(
        constellation_store_s3::LeaseStore::new(
            store.inner().clone(),
            constellation_store_s3::log::PARTITION,
            lease_mode,
        ),
        node_id,
    );
    let lease_views = std::sync::Arc::new(std::sync::Mutex::new({
        let mut m = std::collections::HashMap::new();
        m.insert(
            constellation_store_s3::log::PARTITION.to_string(),
            keeper.view(),
        );
        m
    }));
    // A mutation waits at most ~2 TTLs for a foreign holder to release
    // or expire before failing with EIO.
    let acquire_deadline = std::time::Duration::from_millis(2 * keeper.ttl_ms());

    // Sync task channel: FUSE nudges it on close (publication point),
    // blocks on it for fsync in --fsync-mode s3, and asks it to take
    // the lease on the first mutation.
    let (sync_tx, mut sync_rx) = tokio::sync::mpsc::unbounded_channel::<fusefs::SyncRequest>();
    let fs = fusefs::ConstellationFs::new(
        meta.clone(),
        store.clone(),
        cache.clone(),
        rt.handle().clone(),
        fsmeta.chunk_size,
        compression,
        Some(fusefs::SyncHandle {
            tx: sync_tx.clone(),
            fsync_s3,
            leases: lease_views.clone(),
            acquire_deadline,
        }),
    );

    // Background metadata sync: tail foreign segments + ship the
    // journal, every interval or on demand (close/fsync nudges).
    let interval_ms: u64 = std::env::var("CONSTELLATION_SYNC_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let ship = shipper::Shipper::attach_with_mode(meta.clone(), log, node_id, lease_mode)?;
    let spool = ship.spool.clone();
    let mut ship = ship;
    let mut keeper = keeper;
    rt.block_on(adopt_root(&meta, &mut ship, &mut keeper))
        .context("adopting the root directory owner")?;

    // P2P fast path (DESIGN.md §8, M3.3). Every failure here is
    // non-fatal: without peers the daemon behaves exactly as phases 1-2,
    // reaching other nodes through S3 polling.
    let peers = rt.block_on(start_p2p(&fsmeta, store.inner().clone(), node_id));
    ship.set_peers(peers.clone());
    let ship = std::sync::Arc::new(tokio::sync::Mutex::new(ship));
    let keepers = std::sync::Arc::new(tokio::sync::Mutex::new({
        let mut m = std::collections::HashMap::new();
        m.insert(constellation_store_s3::log::PARTITION.to_string(), keeper);
        m
    }));
    let bridge = std::sync::Arc::new(P2pBridge {
        node_id,
        nudge: sync_tx.clone(),
    });
    if peers.is_enabled() {
        // Refresh-on-miss: an unknown key may be a peer that mounted
        // after us, which on a cold start is the normal case rather than
        // the exception.
        {
            let (p, store_inner) = (peers.clone(), store.inner().clone());
            peers.set_refresher(std::sync::Arc::new(move || {
                let (p, store_inner) = (p.clone(), store_inner.clone());
                Box::pin(async move { refresh_peers(&p, store_inner).await })
            }));
        }
        // Accept inbound peer connections.
        {
            let (peers, bridge) = (peers.clone(), bridge.clone());
            rt.spawn(async move { peers.serve(bridge).await });
        }
        // Join the gossip topic and consume it. Bootstrapping from the
        // registry replaces a global discovery service; the node that
        // mounts first has nobody to bootstrap from, so poll briefly for
        // a peer instead of joining a topic alone.
        {
            let (peers, bridge, store_inner) =
                (peers.clone(), bridge.clone(), store.inner().clone());
            rt.spawn(async move {
                for _ in 0..40 {
                    if !peers.snapshot().is_empty() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    refresh_peers(&peers, store_inner.clone()).await;
                }
                let bootstrap: Vec<constellation_net::EndpointId> =
                    peers.snapshot().iter().map(|p| p.addr.id).collect();
                tracing::info!(bootstrap = bootstrap.len(), "joining the gossip topic");
                match peers.join_topic(bootstrap).await {
                    Ok(rx) => constellation_net::run_gossip(peers.clone(), rx, bridge).await,
                    Err(e) => tracing::warn!(error = %e, "gossip unavailable; peers will poll S3"),
                }
            });
        }
        // Periodically re-read the registry so nodes that join later are
        // dialable and enrolled without a remount.
        {
            let (peers, store_inner) = (peers.clone(), store.inner().clone());
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    refresh_peers(&peers, store_inner.clone()).await;
                }
            });
        }
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let (ship, stop, spool, keepers, lease_views, store_inner, lease_mode, peers) = (
            ship.clone(),
            stop.clone(),
            spool.clone(),
            keepers.clone(),
            lease_views.clone(),
            store.inner().clone(),
            lease_mode,
            peers.clone(),
        );
        rt.spawn(async move {
            let mut pending: Option<fusefs::SyncRequest> = None;
            loop {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let request = if let Some(req) = pending.take() {
                    Some(req)
                } else {
                    tokio::select! {
                        msg = sync_rx.recv() => match msg {
                            Some(req) => Some(req),
                            None => break,
                        },
                        _ = tokio::time::sleep(std::time::Duration::from_millis(interval_ms)) => None,
                    }
                };
                match request {
                    Some(fusefs::SyncRequest::Acquire { part, reply }) => {
                        let mut ship = ship.lock().await;
                        let mut keepers = keepers.lock().await;
                        if !keepers.contains_key(&part) {
                            let k = lease::LeaseKeeper::new(
                                constellation_store_s3::LeaseStore::new(
                                    store_inner.clone(),
                                    &part,
                                    lease_mode,
                                ),
                                node_id,
                            );
                            lease_views.lock().unwrap().insert(part.clone(), k.view());
                            keepers.insert(part.clone(), k);
                        }
                        let keeper = keepers.get_mut(&part).unwrap();
                        let mut r = shipper::acquire_lease_for(&mut ship, keeper, &part).await;
                        // Fast path (M3.3): a live holder can hand the
                        // lease over in ~1 RTT instead of making us wait
                        // out its idle window or TTL. Only worth asking
                        // when the plain CAS just failed, and the retry
                        // is still an ordinary CAS — S3 stays the commit
                        // point, so a lying peer only wastes one round.
                        if matches!(r, Ok(false))
                            && peers.is_enabled()
                            && peers.request_lease(&part).await
                        {
                            r = shipper::acquire_lease_for(&mut ship, keeper, &part).await;
                        }
                        if let Err(e) = &r {
                            tracing::warn!(error = %e, part, "lease acquisition failed");
                        }
                        let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                    }
                    Some(fusefs::SyncRequest::HandOff { part, reply }) => {
                        // Fast-path handoff (M3.3): flush this partition
                        // so the requester sees every committed record,
                        // then release. Declining is always safe — the
                        // requester waits the lease out through S3.
                        let mut ship = ship.lock().await;
                        let mut keepers = keepers.lock().await;
                        let epoch = match keepers.get_mut(&part) {
                            Some(k) if k.ship_epoch().is_some() && !k.is_lost() => {
                                let held = k.ship_epoch();
                                match ship.sync_one(&part, k).await {
                                    Ok(()) => match k.release().await {
                                        Ok(()) => held,
                                        Err(e) => {
                                            tracing::warn!(error = %e, part,
                                                "lease release failed; keeping it");
                                            None
                                        }
                                    },
                                    Err(e) => {
                                        tracing::warn!(error = %e, part,
                                            "flush before handoff failed; keeping the lease");
                                        None
                                    }
                                }
                            }
                            _ => None,
                        };
                        let _ = reply.send(epoch);
                    }
                    Some(fusefs::SyncRequest::Barrier(reply)) => {
                        let r = run_sync_round(&ship, &keepers).await;
                        if let Err(e) = &r {
                            tracing::warn!(error = %e, "metadata sync failed; will retry");
                            spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                        }
                        let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                    }
                    Some(fusefs::SyncRequest::Nudge) | None => {
                        tokio::select! {
                            biased;
                            msg = sync_rx.recv() => {
                                pending = msg;
                            }
                            r = run_sync_round(&ship, &keepers) => {
                                if let Err(e) = r {
                                    tracing::warn!(
                                        error = %e,
                                        "metadata sync failed; will retry"
                                    );
                                    spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // Control API on <state_dir>/control.sock (spool + cache status).
    let status = std::sync::Arc::new(DaemonStatus {
        meta: meta.clone(),
        cache,
        spool,
        leases: lease_views,
        fs_uuid: fsmeta.uuid.to_string(),
        backend: s3.to_string(),
        mountpoint: mountpoint.display().to_string(),
        node_id,
        started: std::time::Instant::now(),
        peers: peers.clone(),
    });
    {
        let _guard = rt.enter();
        if let Err(e) = constellation_api::serve(&state_dir, status) {
            tracing::warn!(error = %e, "control API unavailable");
        }
    }

    let mut options = vec![
        fuser::MountOption::FSName("constellation".into()),
        fuser::MountOption::DefaultPermissions,
    ];
    if allow_other {
        options.push(fuser::MountOption::AllowOther);
    }
    tracing::info!(?mountpoint, ?state_dir, fs = %fsmeta.uuid, "mounting");
    fuser::mount2(fs, mountpoint, &options).context("FUSE mount")?;

    // Clean unmount: ship the journal tail, checkpoint, then release the
    // lease so a peer does not have to wait out the TTL.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let flush = rt.block_on(async {
        let mut ship = ship.lock().await;
        let mut keepers = keepers.lock().await;
        let r = ship.shutdown_all(&mut keepers).await;
        for k in keepers.values_mut() {
            k.release().await?;
        }
        r
    });
    flush.context("final log flush")?;
    Ok(())
}

/// Bridges the P2P layer to the daemon's sync task.
///
/// Both directions are latency-only. A `SegmentPublished` hint just
/// nudges the syncer, which would have polled anyway; a `LeaseRequest`
/// asks the sync task to flush and release, and S3's CAS remains the
/// authority for who actually holds the lease.
struct P2pBridge {
    node_id: u64,
    nudge: tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
}

impl constellation_net::PeerService for P2pBridge {
    fn segment_published(&self, part: &str, seq: u64, epoch: u64) {
        tracing::debug!(part, seq, epoch, "peer published a segment; syncing now");
        // Nudge, never block: if the channel is gone the periodic sync
        // still picks the segment up.
        let _ = self.nudge.send(fusefs::SyncRequest::Nudge);
    }

    fn lease_requested(
        &self,
        part: String,
        requester: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            let declined = constellation_net::Payload::LeaseHandoff {
                part: part.clone(),
                epoch: 0,
                released: false,
            };
            let (tx, rx) = tokio::sync::oneshot::channel();
            if self
                .nudge
                .send(fusefs::SyncRequest::HandOff {
                    part: part.clone(),
                    reply: tx,
                })
                .is_err()
            {
                return declined;
            }
            match rx.await {
                Ok(Some(epoch)) => {
                    tracing::info!(part, requester, epoch, "handed the lease to a peer");
                    constellation_net::Payload::LeaseHandoff {
                        part,
                        epoch,
                        released: true,
                    }
                }
                // Not ours, flush failed, or the task went away: the
                // requester falls back to the S3 path, which is always
                // correct — it just costs the TTL wait.
                _ => declined,
            }
        })
    }

    fn node_id(&self) -> u64 {
        self.node_id
    }
}

/// Start the P2P fast path, or return a disabled handle.
///
/// Everything here is best-effort by design (plan 02 / DESIGN.md §8): a
/// missing node key, an unbindable endpoint, or an unreachable gossip
/// topic all degrade to the S3 polling path rather than failing the
/// mount. `CONSTELLATION_P2P=off` skips it entirely.
async fn start_p2p(
    fsmeta: &constellation_store_s3::FsMeta,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    node_id: u64,
) -> constellation_net::Peers {
    if !constellation_net::enabled() {
        tracing::info!("P2P disabled by CONSTELLATION_P2P; using the S3 path only");
        return constellation_net::Peers::disabled();
    }
    let key_path = constellation_net::identity::default_key_path();
    let (key, generated) = match constellation_net::load_or_create(&key_path) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, path = %key_path.display(),
                "no usable node key; running without the P2P fast path");
            return constellation_net::Peers::disabled();
        }
    };
    if generated {
        tracing::info!(path = %key_path.display(), "generated a host node key");
    }
    let topic =
        constellation_net::topic_for(fsmeta.gossip_seed().as_ref(), &fsmeta.uuid.to_string());
    if fsmeta.gossip_seed().is_none() {
        tracing::info!(
            "filesystem predates gossip_secret; deriving the topic from its UUID \
             (weaker: the UUID is not a secret)"
        );
    }
    let p2p = match constellation_net::P2p::spawn(key, topic).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "could not bind the P2P endpoint; using the S3 path only");
            return constellation_net::Peers::disabled();
        }
    };
    let addr = p2p.addr();
    let pubkey = p2p.pubkey_hex();
    let peers = constellation_net::Peers::new(p2p, node_id);
    // Publish how peers reach us, then learn about them.
    match serde_json::to_value(&addr) {
        Ok(addr_json) => {
            if let Err(e) =
                constellation_store_s3::publish_p2p(store.clone(), node_id, &pubkey, addr_json)
                    .await
            {
                tracing::warn!(error = %e, "could not publish our P2P address; peers cannot dial us");
            }
        }
        Err(e) => tracing::warn!(error = %e, "could not serialize our P2P address"),
    }
    refresh_peers(&peers, store).await;
    tracing::info!(
        node_id,
        peers = peers.snapshot().len(),
        "P2P fast path ready"
    );
    peers
}

/// Re-read the registry into the peer directory and allowlist.
async fn refresh_peers(
    peers: &constellation_net::Peers,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
) {
    if !peers.is_enabled() {
        return;
    }
    match constellation_store_s3::list_nodes(store).await {
        Ok(nodes) => {
            let records: Vec<(u64, String, serde_json::Value)> = nodes
                .into_iter()
                .filter_map(|n| Some((n.node_id, n.pubkey?, n.p2p_addr?)))
                .collect();
            peers.refresh_registry(records);
        }
        Err(e) => {
            tracing::debug!(error = %e, "registry refresh failed; keeping the cached peer set")
        }
    }
}

async fn run_sync_round(
    ship: &std::sync::Arc<tokio::sync::Mutex<shipper::Shipper>>,
    keepers: &std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<String, lease::LeaseKeeper>>,
    >,
) -> Result<()> {
    let mut ship = ship.lock().await;
    let mut keepers = keepers.lock().await;
    let any_lost = keepers.values().any(|k| k.is_lost());
    if any_lost
        && keepers
            .values()
            .all(|k| k.is_lost() || k.ship_epoch().is_none())
    {
        return ship.tail_to_head().await.map(|_| ());
    }
    for k in keepers.values_mut() {
        if !k.is_lost() {
            k.renew_if_due().await?;
        }
    }
    ship.sync_all(&mut keepers).await?;
    for (part, k) in keepers.iter_mut() {
        if k.is_lost() {
            continue;
        }
        if k.idle_release_due(ship.journal_backlog_of(part)) {
            k.release().await?;
        }
    }
    Ok(())
}

/// Give the root directory to the mounting user on a freshly created
/// filesystem. Skipped entirely unless the root is still 0:0, so this
/// costs nothing (and needs no lease) on every subsequent mount; when it
/// does apply, it takes the lease like any other mutation.
async fn adopt_root(
    meta: &std::sync::Arc<SqliteMeta>,
    ship: &mut shipper::Shipper,
    keeper: &mut lease::LeaseKeeper,
) -> Result<()> {
    let euid = unsafe { libc::geteuid() };
    let egid = unsafe { libc::getegid() };
    if euid == 0 {
        return Ok(());
    }
    // Another node may already have done it; make sure we have its log.
    ship.tail_to_head().await?;
    let root =
        constellation_meta::MetaStore::getattr(&**meta, constellation_fs_core::types::ROOT_INO)?;
    if !matches!(root, Some(a) if a.uid == 0) {
        return Ok(());
    }
    if !shipper::acquire_lease(ship, keeper).await? {
        // Another node holds authority; it either already adopted the
        // root or will, and its record reaches us by tailing.
        tracing::info!("root adoption deferred: partition lease held elsewhere");
        return Ok(());
    }
    constellation_meta::MetaStore::setattr(
        &**meta,
        constellation_fs_core::types::ROOT_INO,
        None,
        Some(euid),
        Some(egid),
        None,
        None,
        None,
    )?;
    ship.sync(keeper).await?;
    Ok(())
}

/// Live daemon state exposed over the control socket.
struct DaemonStatus {
    meta: std::sync::Arc<SqliteMeta>,
    cache: std::sync::Arc<DiskCache>,
    spool: std::sync::Arc<std::sync::Mutex<shipper::SpoolInfo>>,
    leases: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<lease::LeaseView>>>,
    >,
    fs_uuid: String,
    backend: String,
    mountpoint: String,
    node_id: u64,
    started: std::time::Instant,
    peers: constellation_net::Peers,
}

impl constellation_api::StatusSource for DaemonStatus {
    fn status(&self) -> constellation_api::StatusReport {
        let spool = self.spool.lock().unwrap().clone();
        let usage = self.cache.usage();
        // Collect everything that needs the lease map BEFORE building the
        // report: temporaries created inside a struct literal live until
        // the whole literal is built, so locking twice in there would
        // self-deadlock the non-reentrant mutex and hang every caller.
        let (p0_lease, partitions) = {
            let views = self.leases.lock().unwrap();
            let p0 = views
                .get(constellation_store_s3::log::PARTITION)
                .map(|v| v.status())
                .unwrap_or_default();
            let parts: Vec<constellation_api::PartitionStatus> = self
                .meta
                .partitions()
                .unwrap_or_default()
                .into_iter()
                .map(|(id, root)| constellation_api::PartitionStatus {
                    root_path: self.meta.path_of(root).unwrap_or_else(|_| "/".into()),
                    lease: views.get(&id).map(|v| v.status()).unwrap_or_default(),
                    id,
                })
                .collect();
            (p0, parts)
        };
        let p2p = constellation_api::P2pStatus {
            enabled: self.peers.is_enabled(),
            node_addr: self
                .peers
                .node_addr()
                .and_then(|a| serde_json::to_string(&a).ok()),
            peers: self
                .peers
                .snapshot()
                .into_iter()
                .map(|p| constellation_api::PeerStatus {
                    node_id: p.node_id,
                    connected: p.connected,
                    rtt_ms: p.rtt_ms,
                })
                .collect(),
        };
        constellation_api::StatusReport {
            fs_uuid: self.fs_uuid.clone(),
            backend: self.backend.clone(),
            mountpoint: self.mountpoint.clone(),
            node_id: self.node_id,
            uptime_s: self.started.elapsed().as_secs(),
            spool: constellation_api::SpoolStatus {
                journal_backlog: constellation_meta::MetaStore::journal_len(&*self.meta)
                    .unwrap_or(0),
                head_seq: spool.head_seq,
                conflicts: spool.conflicts,
                last_ship_error: spool.last_error,
            },
            cache: constellation_api::CacheStatus {
                used_bytes: usage.used,
                budget_bytes: usage.budget,
                chunks: usage.entries as u64,
            },
            lease: p0_lease,
            partitions,
            p2p,
        }
    }
}

fn default_state_dir(meta: &FsMeta) -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_default();
            home.join(".local/share")
        });
    base.join("constellation").join(meta.uuid.to_string())
}
