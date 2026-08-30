//! Constellation entry point: CLI, daemon, and FUSE mount in one binary.

mod backend;
mod coop;
mod designation;
mod epoch;
mod fusefs;
mod lease;
mod leave;
mod pin;
mod prefetch;
mod reintegrate;
mod shipper;
mod sources;
mod staging;

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
        /// Enrol this state directory as a read-only cluster member.
        /// Only valid on its first mount; RO members do not count toward
        /// a continuation epoch's write-eligible roster.
        #[arg(long)]
        read_only_member: bool,
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
    /// Keep a subtree fully cached on this node and follow its changes.
    Pin {
        /// Absolute path inside the filesystem, e.g. /data.
        path: String,
        /// State dir of the running mount to talk to.
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Stop keeping a subtree resident; its chunks become evictable.
    Unpin {
        path: String,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// List this node's pinned subtrees.
    Pins {
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Designate this node for exclusive local-speed writes under a
    /// subtree while it stays reachable (DESIGN.md §5.2).
    Offline {
        path: String,
        #[arg(long)]
        state_dir: PathBuf,
        /// Grant a read guarantee (pin) without write authority; other
        /// nodes' writes under `path` remain unrestricted.
        #[arg(long)]
        ro: bool,
    },
    /// Release this node's designation for a subtree.
    Online {
        path: String,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// List active offline designations visible to this node.
    Designations {
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Reintegrate this node's stranded journal after deposition.
    Reintegrate {
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Permanently leave the cluster (tombstone the registry record).
    /// Omit `--node-id` to leave this mount; pass `--node-id` to retire
    /// a different (unreachable) member via a still-mounted peer.
    Leave {
        #[arg(long)]
        state_dir: PathBuf,
        /// Admin form: retire a different node id.
        #[arg(long)]
        node_id: Option<u64>,
        /// Skip courtesy refusals (live designation / live foreign lease).
        /// Never skips an open epoch or a stranded deposed journal.
        #[arg(long)]
        force: bool,
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
        Command::Pin { path, state_dir } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Pin { path },
        )),
        Command::Unpin { path, state_dir } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Unpin { path },
        )),
        Command::Pins { state_dir } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::ListPins,
        )),
        Command::Offline {
            path,
            state_dir,
            ro,
        } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Offline {
                path,
                read_only: ro,
            },
        )),
        Command::Online { path, state_dir } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Online { path },
        )),
        Command::Designations { state_dir } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::ListDesignations,
        )),
        Command::Reintegrate { state_dir } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Reintegrate,
        )),
        Command::Leave {
            state_dir,
            node_id,
            force,
        } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Leave { node_id, force },
        )),
        Command::Mount {
            s3,
            mountpoint,
            state_dir,
            cache_size,
            allow_other,
            fsync_mode,
            read_only_member,
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
                read_only_member,
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
    read_only_member: bool,
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
    if matches!(meta.kv_get("left")?.as_deref(), Some("1")) {
        bail!(
            "this state directory has permanently left the cluster \
             (kv left=1); mount with a fresh --state-dir to re-enroll \
             under a new node id"
        );
    }
    // Node identity: claim a cluster-unique id on first mount of this
    // state dir; it scopes ino allocation and marks log segment origin.
    let first_mount = meta.kv_get("node_id")?.is_none();
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
    // A remount whose registry record was retired (or deleted) under us
    // must not silently reclaim that id.
    match rt.block_on(constellation_store_s3::get_node(
        store.inner().clone(),
        node_id,
    ))? {
        None => bail!(
            "node {node_id} has no registry record; an operator may have \
             retired it. Use a fresh --state-dir to claim a new id"
        ),
        Some(info) if info.retired => bail!(
            "node {node_id} is retired in the registry; use a fresh \
             --state-dir to re-enroll under a new id"
        ),
        Some(_) => {}
    }
    if first_mount {
        rt.block_on(constellation_store_s3::publish_ro(
            store.inner().clone(),
            node_id,
            read_only_member,
        ))
        .context("publishing read-only membership")?;
        meta.kv_set("read_only_member", if read_only_member { "1" } else { "0" })?;
    } else if read_only_member != matches!(meta.kv_get("read_only_member")?.as_deref(), Some("1")) {
        bail!("--read-only-member is fixed on first mount for this state directory");
    }
    meta.set_node_prefix(node_id)?;
    tracing::info!(node_id, "node identity");

    // Mount-time staging GC (plan 05a step 6): nothing under
    // `staging/` can be live at mount start. A crash mid-write leaves
    // no orphaned staging bytes because this always runs first.
    let staging_dir = state_dir.join("staging");
    let reclaimed = staging::gc(&staging_dir).context("clearing orphaned staging files")?;
    if reclaimed > 0 {
        tracing::info!(
            bytes = reclaimed,
            "reclaimed orphaned staging bytes (previous crash)"
        );
    }
    // Decoupled from --cache-size: staging holds a whole in-flight
    // write until flush/close (05a adds no streaming; that is 05b),
    // while the cache only holds each sealed chunk briefly before eager
    // upload demotes it. Default is a fraction of the cache budget, a
    // reasonable starting point for ordinary interactive workloads;
    // override for large single-file writes.
    let staging_budget_bytes: u64 = std::env::var("CONSTELLATION_STAGING_BUDGET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(cache_size / 4);
    let staging_budget = staging::StagingBudget::new(staging_budget_bytes);

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
    let mut keeper = lease::LeaseKeeper::new(
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

    // P2P fast path (DESIGN.md §8, M3.3). Every failure here is
    // non-fatal: without peers the daemon behaves exactly as phases 1-2,
    // reaching other nodes through S3 polling. Built before `fs` because
    // the offline-designation gate (phase 4a) needs it for delegation
    // requests.
    let peers = rt.block_on(start_p2p(&fsmeta, store.inner().clone(), node_id));
    let epochs = std::sync::Arc::new(epoch::EpochManager::new(
        node_id,
        meta.clone(),
        peers.clone(),
    ));
    keeper.share_takeover_gate(epochs.blocks_takeover.clone());
    let lost_on_mount = matches!(meta.kv_get("lease_lost")?.as_deref(), Some("1"));
    if lost_on_mount {
        keeper.force_lost();
    }
    let roster = rt
        .block_on(constellation_store_s3::write_eligible_roster(
            store.inner().clone(),
        ))
        .context("loading write-eligible roster")?;
    epochs.set_roster(roster);

    let designations = std::sync::Arc::new(designation::DesignationManager::new(
        constellation_store_s3::designation::DesignationStore::new(
            store.inner().clone(),
            if lease_mode == constellation_store_s3::LeaseMode::Cas {
                constellation_store_s3::designation::DesignationMode::Cas
            } else {
                constellation_store_s3::designation::DesignationMode::SingleWriter
            },
        ),
        meta.clone(),
        peers.clone(),
        node_id,
    ));
    rt.block_on(designations.refresh());

    let coop = crate::coop::Coop::new(
        cache.clone(),
        store.clone(),
        peers.clone(),
        node_id,
        fsmeta.chunk_size,
    );

    let departed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fs = fusefs::ConstellationFs::new(
        fusefs::FsDependencies {
            meta: meta.clone(),
            store: store.clone(),
            cache: cache.clone(),
            rt: rt.handle().clone(),
            sync: Some(fusefs::SyncHandle {
                tx: sync_tx.clone(),
                fsync_s3,
                leases: lease_views.clone(),
                acquire_deadline,
                designations: Some(designations.clone()),
                epoch_frozen: Some(epochs.frozen.clone()),
                epoch_active: Some(epochs.active.clone()),
                departed: Some(departed.clone()),
                read_only_member,
            }),
            coop: Some(coop.clone()),
            staging_dir: staging_dir.clone(),
            staging_budget: staging_budget.clone(),
        },
        fsmeta.chunk_size,
        compression,
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
    if !read_only_member {
        rt.block_on(adopt_root(&meta, &mut ship, &mut keeper))
            .context("adopting the root directory owner")?;
    }

    ship.set_peers(peers.clone());
    ship.set_designations(designations.clone());
    let ship = std::sync::Arc::new(tokio::sync::Mutex::new(ship));
    let pins = std::sync::Arc::new(pin::PinManager::new(
        meta.clone(),
        store.clone(),
        cache.clone(),
        Some(coop.clone()),
    ));
    let keepers = std::sync::Arc::new(tokio::sync::Mutex::new({
        let mut m = std::collections::HashMap::new();
        m.insert(constellation_store_s3::log::PARTITION.to_string(), keeper);
        m
    }));
    let reintegration = std::sync::Arc::new(reintegrate::ReintegrationState::default());
    // Only a persisted deposition is known to be a stranded branch.
    // Ordinary crash-recovery journals must retain their existing
    // ship-in-place path; treating every pending row as deposed would
    // unnecessarily rebuild a healthy replica on each remount.
    let reintegrate_on_mount = lost_on_mount && !epochs.is_open();
    let bridge = std::sync::Arc::new(P2pBridge {
        node_id,
        nudge: sync_tx.clone(),
        designations: designations.clone(),
        meta: meta.clone(),
        epochs: epochs.clone(),
        coop: coop.clone(),
    });
    if peers.is_enabled() {
        // Refresh-on-miss: an unknown key may be a peer that mounted
        // after us, which on a cold start is the normal case rather than
        // the exception.
        {
            let (p, store_inner, epochs) = (peers.clone(), store.inner().clone(), epochs.clone());
            peers.set_refresher(std::sync::Arc::new(move || {
                let (p, store_inner, epochs) = (p.clone(), store_inner.clone(), epochs.clone());
                Box::pin(async move { refresh_peers(&p, store_inner, Some(&epochs)).await })
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
            let (peers, bridge, store_inner, epochs) = (
                peers.clone(),
                bridge.clone(),
                store.inner().clone(),
                epochs.clone(),
            );
            rt.spawn(async move {
                for _ in 0..40 {
                    if !peers.snapshot().is_empty() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    refresh_peers(&peers, store_inner.clone(), Some(&epochs)).await;
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
        // Cooperative-cache digest publisher (DESIGN.md §7).
        {
            let coop = coop.clone();
            rt.spawn(async move { coop.publish_loop().await });
        }
        // Periodically re-read the registry so nodes that join later are
        // dialable and enrolled without a remount. Also detect our own
        // record vanishing or being retired (admin leave under us).
        {
            let (peers, store_inner, epochs, departed, node_id, meta) = (
                peers.clone(),
                store.inner().clone(),
                epochs.clone(),
                departed.clone(),
                node_id,
                meta.clone(),
            );
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    refresh_peers(&peers, store_inner.clone(), Some(&epochs)).await;
                    match constellation_store_s3::get_node(store_inner.clone(), node_id).await {
                        Ok(None) => {
                            tracing::error!(
                                node_id,
                                "our registry record vanished; stopping writes \
                                 (operator admin-leave?). remount with a fresh state dir"
                            );
                            departed.store(true, std::sync::atomic::Ordering::Relaxed);
                            let _ = meta.kv_set("left", "1");
                        }
                        Ok(Some(info)) if info.retired => {
                            tracing::error!(
                                node_id,
                                "our registry record is retired; stopping writes"
                            );
                            departed.store(true, std::sync::atomic::Ordering::Relaxed);
                            let _ = meta.kv_set("left", "1");
                        }
                        Ok(_) => {}
                        Err(e) => tracing::debug!(error = %e, "own-record membership check failed"),
                    }
                }
            });
        }
    } else {
        // P2P off: still refresh the epoch roster and watch our own record.
        let (store_inner, epochs, departed, node_id, meta) = (
            store.inner().clone(),
            epochs.clone(),
            departed.clone(),
            node_id,
            meta.clone(),
        );
        rt.spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                match constellation_store_s3::write_eligible_roster(store_inner.clone()).await {
                    Ok(roster) => epochs.set_roster(roster),
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "cannot determine the write-eligible roster; \
                             continuation epochs stay unavailable"
                        );
                        epochs.set_roster(Vec::new());
                    }
                }
                match constellation_store_s3::get_node(store_inner.clone(), node_id).await {
                    Ok(None) => {
                        tracing::error!(node_id, "our registry record vanished; stopping writes");
                        departed.store(true, std::sync::atomic::Ordering::Relaxed);
                        let _ = meta.kv_set("left", "1");
                    }
                    Ok(Some(info)) if info.retired => {
                        tracing::error!(node_id, "our registry record is retired; stopping writes");
                        departed.store(true, std::sync::atomic::Ordering::Relaxed);
                        let _ = meta.kv_set("left", "1");
                    }
                    _ => {}
                }
            }
        });
    }
    // Designations are rare, operator-driven objects, but the daemon
    // must notice one appear/disappear without a remount (e.g. another
    // node ran `offline`). Poll less aggressively than the peer
    // registry since S3 LIST is not free and there is no gossip signal
    // for this yet.
    {
        let designations = designations.clone();
        rt.spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                designations.refresh().await;
            }
        });
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let (
            ship,
            stop,
            spool,
            keepers,
            lease_views,
            store_inner,
            lease_mode,
            peers,
            pins,
            epochs,
            meta,
            cache,
            chunk_store,
            reintegration,
            state_dir,
            designations,
        ) = (
            ship.clone(),
            stop.clone(),
            spool.clone(),
            keepers.clone(),
            lease_views.clone(),
            store.inner().clone(),
            lease_mode,
            peers.clone(),
            pins.clone(),
            epochs.clone(),
            meta.clone(),
            cache.clone(),
            store.clone(),
            reintegration.clone(),
            state_dir.clone(),
            designations.clone(),
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
                        let deposed = match meta.kv_get("lease_lost") {
                            Ok(value) => matches!(value.as_deref(), Some("1")),
                            Err(error) => {
                                let _ = reply.send(Err(format!(
                                    "cannot read persisted deposition state: {error}"
                                )));
                                continue;
                            }
                        };
                        if deposed {
                            let _ = reply.send(Err(
                                "this node was deposed; run reintegration before acquiring leases"
                                    .into(),
                            ));
                            continue;
                        }
                        let mut ship = ship.lock().await;
                        let mut keepers = keepers.lock().await;
                        if !keepers.contains_key(&part) {
                            let mut k = lease::LeaseKeeper::new(
                                constellation_store_s3::LeaseStore::new(
                                    store_inner.clone(),
                                    &part,
                                    lease_mode,
                                ),
                                node_id,
                            );
                            k.share_takeover_gate(epochs.blocks_takeover.clone());
                            lease_views.lock().unwrap().insert(part.clone(), k.view());
                            keepers.insert(part.clone(), k);
                        }
                        let keeper = keepers.get_mut(&part).unwrap();
                        let mut r = if epochs.writes_ok() {
                            if keeper.holds_authority() {
                                Ok(true)
                            } else if peers.request_lease(&part).await {
                                keeper.adopt_epoch_hold(keeper.authority_epoch());
                                Ok(true)
                            } else {
                                Ok(false)
                            }
                        } else {
                            shipper::acquire_lease_for(&mut ship, keeper, &part).await
                        };
                        // Fast path (M3.3): a live holder can hand the
                        // lease over in ~1 RTT instead of making us wait
                        // out its idle window or TTL. Only worth asking
                        // when the plain CAS just failed, and the retry
                        // is still an ordinary CAS — S3 stays the commit
                        // point, so a lying peer only wastes one round.
                        if !epochs.is_open()
                            && matches!(r, Ok(false))
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
                                if epochs.writes_ok() {
                                    k.release_local();
                                    held
                                } else if let Err(e) =
                                    upload_dirty_chunks(&cache, &meta, &chunk_store, compression).await
                                {
                                    tracing::warn!(
                                        error = %e,
                                        part,
                                        "dirty chunk upload before handoff failed; keeping the lease"
                                    );
                                    None
                                } else {
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
                            }
                            _ => None,
                        };
                        let _ = reply.send(epoch);
                    }
                    Some(fusefs::SyncRequest::Barrier(reply)) => {
                        let r = run_managed_sync_round(
                            &ship,
                            &keepers,
                            &epochs,
                            &meta,
                            &cache,
                            &chunk_store,
                            compression,
                        )
                        .await;
                        if let Err(e) = &r {
                            tracing::warn!(error = %e, "metadata sync failed; will retry");
                            spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                        } else {
                            pins.refresh_all().await;
                        }
                        let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                    }
                    Some(fusefs::SyncRequest::Reintegrate(reply)) => {
                        if epochs.is_open() {
                            let _ = reply.send(Err(
                                "cannot reintegrate while a continuation epoch is open".into(),
                            ));
                            continue;
                        }
                        let mut ship = ship.lock().await;
                        let mut keepers = keepers.lock().await;
                        let r = reintegrate::run(
                            &meta,
                            &mut ship,
                            &mut keepers,
                            node_id,
                            &state_dir,
                            &reintegration,
                        )
                        .await;
                        let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                    }
                    Some(fusefs::SyncRequest::Leave { force, reply }) => {
                        if let Err(e) = leave::refuse_open_epoch(&epochs) {
                            let _ = reply.send(Err(e.to_string()));
                            continue;
                        }
                        // Upload dirty chunks before the journal flush so
                        // self-leave does not strand content that only
                        // exists in the local cache.
                        if let Err(e) =
                            upload_dirty_chunks(&cache, &meta, &chunk_store, compression).await
                        {
                            let _ = reply.send(Err(format!(
                                "cannot upload dirty chunks before leave: {e:#}"
                            )));
                            continue;
                        }
                        let mut ship = ship.lock().await;
                        let mut keepers = keepers.lock().await;
                        let r = leave::self_leave(
                            store_inner.clone(),
                            &meta,
                            &mut ship,
                            &mut keepers,
                            &designations,
                            node_id,
                            force,
                        )
                        .await;
                        let _ = reply.send(r.map(|_| format!(
                            "left cluster as node {node_id}; registry record retired"
                        )).map_err(|e| e.to_string()));
                    }
                    Some(fusefs::SyncRequest::Nudge) | None => {
                        tokio::select! {
                            biased;
                            msg = sync_rx.recv() => {
                                pending = msg;
                            }
                            r = run_managed_sync_round(
                                &ship,
                                &keepers,
                                &epochs,
                                &meta,
                                &cache,
                                &chunk_store,
                                compression,
                            ) => {
                                if let Err(e) = r {
                                    tracing::warn!(
                                        error = %e,
                                        "metadata sync failed; will retry"
                                    );
                                    spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                                } else {
                                    pins.refresh_all().await;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    if reintegrate_on_mount {
        let (reply, receive) = tokio::sync::oneshot::channel();
        sync_tx
            .send(fusefs::SyncRequest::Reintegrate(reply))
            .map_err(|_| anyhow::anyhow!("sync task stopped before automatic reintegration"))?;
        rt.block_on(receive)
            .context("automatic reintegration task stopped")?
            .map_err(anyhow::Error::msg)
            .context("automatic reintegration after mount")?;
    }

    let status = std::sync::Arc::new(DaemonStatus {
        meta: meta.clone(),
        cache: cache.clone(),
        staging_budget: staging_budget.clone(),
        spool,
        leases: lease_views,
        fs_uuid: fsmeta.uuid.to_string(),
        backend: s3.to_string(),
        mountpoint: mountpoint.display().to_string(),
        node_id,
        started: std::time::Instant::now(),
        peers: peers.clone(),
        pins: pins.clone(),
        designations: designations.clone(),
        epochs: epochs.clone(),
        reintegration: reintegration.clone(),
        sync_tx: sync_tx.clone(),
        store: store.inner().clone(),
        departed: departed.clone(),
        rt: rt.handle().clone(),
        coop: coop.clone(),
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
    // lease so a peer does not have to wait out the TTL. Skip when we
    // already flushed and retired via `leave` — the registry record is
    // a tombstone and a second ship is unnecessary.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    if matches!(meta.kv_get("left")?.as_deref(), Some("1")) {
        return Ok(());
    }
    let flush = rt.block_on(async {
        // Plan 05a step 2: an orderly unmount must not publish manifests
        // for chunks that never made it to S3. If a previous best-effort
        // eager upload (`try_upload_dirty`) failed and only logged, this
        // is the last chance to drain `pending_upload` before the
        // journal ships — an unmount that refuses to finish cleanly here
        // is strictly better than one that silently strands content.
        if let Err(e) = upload_dirty_chunks(&cache, &meta, &store, compression).await {
            ship.lock().await.set_skip_ship(true);
            return Err(e).context(
                "uploading dirty chunks before unmount; the journal was left un-shipped \
                 (run `constellation status --state-dir ...` after remounting to drain it)",
            );
        }
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
    designations: std::sync::Arc<designation::DesignationManager>,
    meta: std::sync::Arc<SqliteMeta>,
    epochs: std::sync::Arc<epoch::EpochManager>,
    coop: std::sync::Arc<crate::coop::Coop>,
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

    fn delegation_requested(
        &self,
        path: String,
        requester: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            self.designations
                .handle_delegation_request(&path, requester)
        })
    }

    fn flush_ack_requested(
        &self,
        path: String,
        part: String,
        seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            // We can ack once our own replica has tailed at least this
            // seq for the partition — that IS "provably holds all
            // committed changes" for this record. `applied_seq_of`
            // reads the same kv counter the syncer advances after
            // applying (or shipping) a segment.
            let applied = self.meta.applied_seq_of(&part).unwrap_or(0);
            constellation_net::Payload::FlushAck {
                path,
                part,
                seq,
                acked: applied >= seq,
            }
        })
    }

    fn epoch_proposed(
        &self,
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
        proposer: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            self.epochs
                .handle_propose(epoch_id, members, base, proposer)
        })
    }

    fn epoch_activated(&self, epoch_id: String, members: Vec<u64>, base: Vec<(String, u64)>) {
        self.epochs.handle_activate(epoch_id, members, base);
        let _ = self.nudge.send(fusefs::SyncRequest::Nudge);
    }

    fn cache_digest(&self, digest: constellation_net::DigestSnapshot) {
        self.coop.apply_digest(digest);
    }

    fn cache_digest_delta(&self, delta: constellation_net::DigestDelta) {
        self.coop.apply_delta(delta);
    }

    fn serve_chunk(
        &self,
        hash: [u8; 32],
        from_hex: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Vec<u8>>> + Send + '_>> {
        Box::pin(async move { self.coop.serve_chunk(hash, &from_hex).await })
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
    refresh_peers(&peers, store, None).await;
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
    epochs: Option<&epoch::EpochManager>,
) {
    if !peers.is_enabled() {
        return;
    }
    // The peer directory is tolerant of unreadable records (a peer we
    // cannot dial only loses its fast path); the epoch roster is not,
    // so it gets its own fail-closed read. On failure the roster is
    // cleared rather than left stale: an empty roster can never satisfy
    // `component_covers_roster`, so no epoch activates on a registry we
    // could not fully read.
    if let Some(epochs) = epochs {
        match constellation_store_s3::write_eligible_roster(store.clone()).await {
            Ok(roster) => epochs.set_roster(roster),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "cannot determine the write-eligible roster; \
                     continuation epochs stay unavailable"
                );
                epochs.set_roster(Vec::new());
            }
        }
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

/// Send one control-API request to a running mount and print the answer.
///
/// A daemon-side refusal (over budget, no such path) comes back as
/// `Response::Error` and must exit non-zero: `pin` failing silently would
/// leave the user believing a subtree is resident when it is not.
async fn control_call(state_dir: &std::path::Path, req: constellation_api::Request) -> Result<()> {
    let sock = state_dir.join(constellation_api::SOCKET_NAME);
    match constellation_api::call(&sock, &req).await? {
        constellation_api::Response::Ok { detail } => {
            println!("{detail}");
            Ok(())
        }
        constellation_api::Response::Pins { pins } => {
            if pins.is_empty() {
                println!("no pinned subtrees");
            } else {
                println!("{}", serde_json::to_string_pretty(&pins)?);
            }
            Ok(())
        }
        constellation_api::Response::Designations { designations } => {
            if designations.is_empty() {
                println!("no active designations");
            } else {
                println!("{}", serde_json::to_string_pretty(&designations)?);
            }
            Ok(())
        }
        constellation_api::Response::Error { message } => bail!("{message}"),
        other => bail!("unexpected response: {other:?}"),
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

/// Drain `SqliteMeta::pending_uploads()` — the durable not-yet-uploaded
/// set (plan 05a step 1) — rather than `DiskCache::dirty_chunks()`,
/// which cannot survive a crash (`DiskCache::rescan` legitimately marks
/// everything `Clean`; only the meta journal's transaction-coupled
/// table knows what still owes S3 a PUT).
///
/// A pending row whose chunk is missing from the local cache is
/// unrecoverable content (a torn-disk case, impossible on a clean crash
/// given the write ordering `flush_inode` uses): log loudly, leave the
/// row, and refuse rather than silently drop it — returning an error
/// here already makes every caller treat the round as failed and skip
/// shipping (see `run_managed_sync_round`'s existing epoch-propose-on-
/// failure path), which is exactly the "journal must wait" behavior.
async fn upload_dirty_chunks(
    cache: &DiskCache,
    meta: &SqliteMeta,
    store: &ChunkStore,
    compression: CompressionSetting,
) -> Result<()> {
    for (hash, ino) in meta.pending_uploads()? {
        let Some(data) = cache.get(&hash)? else {
            tracing::error!(
                %hash,
                ino,
                "pending upload chunk missing from local cache (unrecoverable content); \
                 leaving the pending row and refusing to ship"
            );
            bail!("pending upload chunk {hash} for ino {ino} missing from local cache");
        };
        store.put_chunk(&hash, &data, compression).await?;
        cache.set_state(&hash, constellation_fs_core::cache::ChunkState::Clean);
        meta.ack_upload(&hash, ino)?;
    }
    Ok(())
}

/// Drive either the ordinary S3 authority path or a continuation epoch.
/// An S3 failure may activate an epoch, but the failing round remains an
/// error for spool observability. While active, a successful tail probe
/// means S3 returned: upload dirty chunks first, close the promise, then
/// resume ordinary CAS-serialized shipping.
async fn run_managed_sync_round(
    ship: &std::sync::Arc<tokio::sync::Mutex<shipper::Shipper>>,
    keepers: &std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<String, lease::LeaseKeeper>>,
    >,
    epochs: &epoch::EpochManager,
    meta: &SqliteMeta,
    cache: &DiskCache,
    store: &ChunkStore,
    compression: CompressionSetting,
) -> Result<()> {
    if epochs.is_open() {
        {
            let mut keepers = keepers.lock().await;
            for keeper in keepers.values_mut().filter(|k| k.holds_authority()) {
                let authority_epoch = keeper.authority_epoch();
                keeper.adopt_epoch_hold(authority_epoch);
            }
        }
        epochs.check_liveness().await;
        if epochs.is_frozen() {
            return Ok(());
        }
        let s3_back = {
            let mut ship = ship.lock().await;
            ship.tail_to_head().await.is_ok()
        };
        if !s3_back {
            return Ok(());
        }
        let holds_epoch_lease = keepers
            .lock()
            .await
            .values()
            .any(|keeper| keeper.holds_authority());
        if !holds_epoch_lease && !epochs.shared_log_advanced(&meta.applied_vector()?) {
            // The current epoch holder must publish first. A previous
            // holder keeps its promise open until it has tailed that
            // publication, then follows with its older local journal.
            return Ok(());
        }
        upload_dirty_chunks(cache, meta, store, compression).await?;
        {
            let ship = ship.lock().await;
            ship.set_skip_ship(false);
        }
        epochs.close();
        {
            let mut keepers = keepers.lock().await;
            for keeper in keepers.values_mut() {
                keeper.release_local();
            }
        }
        let result = run_sync_round(ship, keepers).await;
        if result.is_ok() {
            let ship = ship.lock().await;
            let mut keepers = keepers.lock().await;
            let mut drained = true;
            for (part, keeper) in keepers.iter_mut() {
                if ship.journal_backlog_of(part) == 0 {
                    keeper.release().await?;
                } else {
                    drained = false;
                }
            }
            if drained {
                epochs.finish_flushing();
            }
            // Keep the guard alive only long enough to read backlog;
            // release above is S3-only and does not call into shipper.
            drop(ship);
        }
        return result;
    }

    if let Err(error) = upload_dirty_chunks(cache, meta, store, compression).await {
        let base = meta.applied_vector()?;
        if epochs.maybe_propose(base).await? {
            let ship = ship.lock().await;
            ship.set_skip_ship(true);
            drop(ship);
            let mut keepers = keepers.lock().await;
            for keeper in keepers.values_mut().filter(|k| k.holds_authority()) {
                let authority_epoch = keeper.authority_epoch();
                keeper.adopt_epoch_hold(authority_epoch);
            }
        }
        return Err(error);
    }

    let result = run_sync_round(ship, keepers).await;
    if result.is_ok() {
        epochs.note_s3_success();
    }
    if keepers.lock().await.values().any(|k| k.is_lost()) {
        meta.kv_set("lease_lost", "1")?;
    }
    if result.is_ok() && epochs.is_flushing() {
        let ship = ship.lock().await;
        let drained = {
            let keepers = keepers.lock().await;
            keepers
                .keys()
                .all(|part| ship.journal_backlog_of(part) == 0)
        };
        if drained {
            let mut keepers = keepers.lock().await;
            for keeper in keepers.values_mut() {
                keeper.release().await?;
            }
            epochs.finish_flushing();
        }
    }
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let base = meta.applied_vector()?;
            if epochs.maybe_propose(base).await? {
                let ship = ship.lock().await;
                ship.set_skip_ship(true);
                drop(ship);
                let mut keepers = keepers.lock().await;
                for keeper in keepers.values_mut().filter(|k| k.holds_authority()) {
                    let epoch = keeper.authority_epoch();
                    keeper.adopt_epoch_hold(epoch);
                }
            }
            Err(error)
        }
    }
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
    staging_budget: std::sync::Arc<staging::StagingBudget>,
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
    pins: std::sync::Arc<pin::PinManager>,
    designations: std::sync::Arc<designation::DesignationManager>,
    epochs: std::sync::Arc<epoch::EpochManager>,
    reintegration: std::sync::Arc<reintegrate::ReintegrationState>,
    sync_tx: tokio::sync::mpsc::UnboundedSender<fusefs::SyncRequest>,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    departed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Handle for the blocking control-API calls that need to await.
    rt: tokio::runtime::Handle,
    coop: std::sync::Arc<crate::coop::Coop>,
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
        let enrolled = !self.departed.load(std::sync::atomic::Ordering::Relaxed)
            && !matches!(
                self.meta.kv_get("left").ok().flatten().as_deref(),
                Some("1")
            );
        constellation_api::StatusReport {
            fs_uuid: self.fs_uuid.clone(),
            backend: self.backend.clone(),
            mountpoint: self.mountpoint.clone(),
            node_id: self.node_id,
            enrolled,
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
                staging_bytes: self.staging_budget.used(),
                staging_budget_bytes: self.staging_budget.budget(),
            },
            lease: p0_lease,
            partitions,
            p2p,
            pins: self.list_pins(),
            designations: self.list_designations(),
            epoch: self.epochs.status(),
            reintegration: self.reintegration.snapshot(
                self.meta
                    .unmarked_journal()
                    .map(|rows| rows.len() as u64)
                    .unwrap_or(0),
            ),
            coop: self.coop.report(),
        }
    }

    fn pin(&self, path: &str) -> std::result::Result<String, String> {
        let pins = self.pins.clone();
        let path = path.to_string();
        // The API handler runs on the runtime already, so block_in_place
        // keeps the fetch off the async executor without a nested runtime.
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { pins.pin(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn unpin(&self, path: &str) -> std::result::Result<String, String> {
        let pins = self.pins.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { pins.unpin(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn list_pins(&self) -> Vec<constellation_api::PinStatus> {
        let pins = self.pins.clone();
        tokio::task::block_in_place(|| self.rt.block_on(async move { pins.status().await }))
    }

    fn offline(&self, path: &str, read_only: bool) -> std::result::Result<String, String> {
        let designations = self.designations.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { designations.offline(&path, read_only).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn online(&self, path: &str) -> std::result::Result<String, String> {
        let designations = self.designations.clone();
        let path = path.to_string();
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(async move { designations.online(&path).await })
                .map_err(|e| format!("{e:#}"))
        })
    }

    fn list_designations(&self) -> Vec<constellation_api::DesignationStatus> {
        self.designations
            .snapshot()
            .into_iter()
            .map(|d| constellation_api::DesignationStatus {
                path: d.path,
                designee: d.designee,
                read_only: d.read_only,
            })
            .collect()
    }

    fn reintegrate(&self) -> std::result::Result<String, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Reintegrate(reply))
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(receive)
                .map_err(|_| "reintegration task stopped".to_string())?
        })
    }

    fn leave(&self, node_id: Option<u64>, force: bool) -> std::result::Result<String, String> {
        match node_id {
            Some(target) => {
                let store = self.store.clone();
                let designations = self.designations.clone();
                let self_id = self.node_id;
                tokio::task::block_in_place(|| {
                    self.rt.block_on(async move {
                        leave::admin_leave(store, &designations, self_id, target, force)
                            .await
                            .map(|_| format!("retired node {target} in the registry"))
                            .map_err(|e| e.to_string())
                    })
                })
            }
            None => {
                leave::refuse_open_epoch(&self.epochs).map_err(|e| e.to_string())?;
                let (reply, receive) = tokio::sync::oneshot::channel();
                self.sync_tx
                    .send(fusefs::SyncRequest::Leave { force, reply })
                    .map_err(|_| "sync task is not running".to_string())?;
                let detail = tokio::task::block_in_place(|| {
                    self.rt
                        .block_on(receive)
                        .map_err(|_| "leave task stopped".to_string())?
                })?;
                self.departed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                // Unmount after the response is on the wire: the control
                // handler writes the Ok then this returns, then we detach.
                let mp = self.mountpoint.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    let _ = std::process::Command::new("fusermount3")
                        .args(["-u"])
                        .arg(&mp)
                        .status();
                });
                Ok(detail)
            }
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

/// Plan 05a's `pending_upload`-driven regression tests for
/// `upload_dirty_chunks`: the durable not-yet-uploaded set must survive
/// a crash even though `DiskCache::rescan` legitimately reports every
/// rediscovered chunk `Clean` (prerequisite 1), and a failed drain must
/// leave the pending row rather than silently dropping it (the "journal
/// must wait" state that backs prerequisite 2's unmount gate).
#[cfg(test)]
mod pending_upload_tests {
    use super::*;
    use constellation_fs_core::cache::ChunkState;
    use constellation_fs_core::ChunkHash;
    use constellation_meta::MetaStore;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// Wraps an in-memory backend and can be told to fail every `put`,
    /// simulating a cut S3 path without needing toxiproxy for a unit
    /// test.
    #[derive(Debug)]
    struct FailingStore {
        inner: InMemory,
        fail_puts: AtomicBool,
    }

    impl FailingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                fail_puts: AtomicBool::new(false),
            })
        }

        fn set_fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for FailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if self.fail_puts.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "FailingStore",
                    source: "S3 path is cut (test injection)".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &ObjPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &ObjPath,
            to: &ObjPath,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    struct Fixture {
        meta: SqliteMeta,
        cache: DiskCache,
        cache_dir: PathBuf,
        store: ChunkStore,
        failing: Arc<FailingStore>,
        _cache_tmp: tempfile::TempDir,
    }

    impl Fixture {
        /// Reopen the cache from the same directory: `DiskCache::open`
        /// rebuilds accounting purely from what is on disk, the same
        /// path a real remount after `kill -9` takes.
        fn reopen_cache_simulating_crash(&mut self) {
            self.cache = DiskCache::open(&self.cache_dir, 64 * 1024 * 1024).unwrap();
        }
    }

    fn fixture() -> Fixture {
        let failing = FailingStore::new();
        let cache_tmp = tempfile::tempdir().unwrap();
        let cache_dir = cache_tmp.path().to_path_buf();
        Fixture {
            meta: SqliteMeta::open_in_memory().unwrap(),
            cache: DiskCache::open(&cache_dir, 64 * 1024 * 1024).unwrap(),
            cache_dir,
            store: ChunkStore::new(failing.clone() as Arc<dyn ObjectStore>),
            failing,
            _cache_tmp: cache_tmp,
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// Regression test for prerequisite 1: after a "crash",
    /// `DiskCache::rescan` reports the chunk `Clean` (it cannot tell
    /// uploaded from un-uploaded content from a directory listing
    /// alone), but the durable `pending_upload` row must still drive the
    /// drain to completion.
    #[test]
    fn drain_finds_pending_row_even_though_cache_reports_clean_after_rescan() {
        let mut f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let data = b"post-crash content".to_vec();
        let hash = ChunkHash::of(&data);
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, b"M", data.len() as u64, &[hash])
            .unwrap();

        // Simulate the crash: reopening the cache rebuilds accounting
        // purely from disk and legitimately reports Clean (see
        // `cache::tests::rescan_rebuilds_accounting`).
        f.reopen_cache_simulating_crash();
        assert_eq!(f.cache.state_of(&hash), Some(ChunkState::Clean));
        assert_eq!(f.meta.pending_uploads().unwrap(), vec![(hash, file.ino)]);

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
        ))
        .unwrap();

        assert!(
            f.meta.pending_uploads().unwrap().is_empty(),
            "drain must ack the pending row once the chunk is uploaded"
        );
        let uploaded = rt().block_on(f.store.get_chunk(&hash)).unwrap();
        assert_eq!(uploaded, data);
    }

    /// Regression test for prerequisite 2's failure mode: while S3 is
    /// unreachable, the drain must refuse (not silently drop the
    /// pending row), which is exactly the signal the clean-unmount path
    /// uses to call `set_skip_ship(true)` rather than shipping a
    /// manifest for content that never reached S3.
    #[test]
    fn failed_drain_leaves_the_pending_row_for_the_next_attempt() {
        let f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let data = b"never uploaded".to_vec();
        let hash = ChunkHash::of(&data);
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, b"M", data.len() as u64, &[hash])
            .unwrap();

        f.failing.set_fail_puts(true);
        let err = rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
        ));
        assert!(err.is_err(), "drain must fail while S3 is unreachable");
        assert_eq!(
            f.meta.pending_uploads().unwrap(),
            vec![(hash, file.ino)],
            "a failed drain must not ack the row it could not upload"
        );

        // Heal, retry: the very next attempt must succeed and ack.
        f.failing.set_fail_puts(false);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
        ))
        .unwrap();
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }
}
