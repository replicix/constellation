//! Constellation entry point: CLI, daemon, and FUSE mount in one binary.

mod backend;
mod fusefs;
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
                println!("note: etag CAS unavailable; fine for single-node (phase 1), required for leases");
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
        } => mount(rt, &s3, &mountpoint, state_dir, cache_size, allow_other),
    }
}

fn mount(
    rt: tokio::runtime::Runtime,
    s3: &str,
    mountpoint: &std::path::Path,
    state_dir: Option<PathBuf>,
    cache_size: u64,
    allow_other: bool,
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
    // First mount: adopt the mounting user as owner of the root directory
    // (the DB bootstraps it as 0:0).
    let euid = unsafe { libc::geteuid() };
    let egid = unsafe { libc::getegid() };
    if let Some(root) =
        constellation_meta::MetaStore::getattr(&*meta, constellation_fs_core::types::ROOT_INO)?
    {
        if root.uid == 0 && euid != 0 {
            constellation_meta::MetaStore::setattr(
                &*meta,
                constellation_fs_core::types::ROOT_INO,
                None,
                Some(euid),
                Some(egid),
                None,
                None,
                None,
            )?;
        }
    }
    let cache = std::sync::Arc::new(DiskCache::open(state_dir.join("cache"), cache_size)?);
    let compression: CompressionSetting = fsmeta
        .compression
        .parse()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let fs = fusefs::ConstellationFs::new(
        meta.clone(),
        store,
        cache.clone(),
        rt.handle().clone(),
        fsmeta.chunk_size,
        compression,
    );

    // Background log shipping: journal -> S3 segments every 2 s.
    let ship = rt.block_on(shipper::Shipper::attach(meta.clone(), log))?;
    let spool = ship.spool.clone();
    let ship = std::sync::Arc::new(tokio::sync::Mutex::new(ship));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let (ship, stop, spool) = (ship.clone(), stop.clone(), spool.clone());
        rt.spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                // Drain the whole journal, not just one batch: churny
                // workloads produce records faster than one batch/tick.
                let r = async {
                    let mut ship = ship.lock().await;
                    while ship.flush().await? {}
                    anyhow::Ok(())
                }
                .await;
                if let Err(e) = r {
                    // Transient S3 failures: the journal retains the
                    // records; the next tick retries (DESIGN.md §12).
                    tracing::warn!(error = %e, "log shipping failed; will retry");
                    spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                }
            }
        });
    }

    // Control API on <state_dir>/control.sock (spool + cache status).
    let status = std::sync::Arc::new(DaemonStatus {
        meta,
        cache,
        spool,
        fs_uuid: fsmeta.uuid.to_string(),
        backend: s3.to_string(),
        mountpoint: mountpoint.display().to_string(),
        started: std::time::Instant::now(),
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

    // Clean unmount: ship the journal tail and checkpoint.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    rt.block_on(async { ship.lock().await.shutdown().await })
        .context("final log flush")?;
    Ok(())
}

/// Live daemon state exposed over the control socket.
struct DaemonStatus {
    meta: std::sync::Arc<SqliteMeta>,
    cache: std::sync::Arc<DiskCache>,
    spool: std::sync::Arc<std::sync::Mutex<shipper::SpoolInfo>>,
    fs_uuid: String,
    backend: String,
    mountpoint: String,
    started: std::time::Instant,
}

impl constellation_api::StatusSource for DaemonStatus {
    fn status(&self) -> constellation_api::StatusReport {
        let spool = self.spool.lock().unwrap().clone();
        let usage = self.cache.usage();
        constellation_api::StatusReport {
            fs_uuid: self.fs_uuid.clone(),
            backend: self.backend.clone(),
            mountpoint: self.mountpoint.clone(),
            uptime_s: self.started.elapsed().as_secs(),
            spool: constellation_api::SpoolStatus {
                journal_backlog: constellation_meta::MetaStore::journal_len(&*self.meta)
                    .unwrap_or(0),
                shipped_seq: spool.shipped_seq,
                last_ship_error: spool.last_error,
            },
            cache: constellation_api::CacheStatus {
                used_bytes: usage.used,
                budget_bytes: usage.budget,
                chunks: usage.entries as u64,
            },
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
