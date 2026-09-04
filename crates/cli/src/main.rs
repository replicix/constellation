//! Constellation entry point: CLI, daemon, and FUSE mount in one binary.

mod backend;
mod coop;
mod designation;
mod epoch;
mod existence;
mod forward;
mod fsck;
mod fusefs;
mod gc;
mod lease;
mod leave;
mod log_buffer;
mod parallelism;
mod pin;
mod placement;
mod prefetch;
mod reintegrate;
mod shipper;
mod snapshot;
mod sources;
mod staging;
mod writeback;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::SqliteMeta;
use constellation_store_s3::{
    change_passphrase, load_keyring, put_keyring, ChunkStore, CompressionSetting, FsMeta,
    StoreError,
};
use std::path::PathBuf;
use zeroize::Zeroizing;

#[derive(Parser)]
#[command(
    name = "constellation",
    version = env!("CONSTELLATION_VERSION"),
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
        /// Legacy form: `<mountpoint>`. Subtree form:
        /// `<inner-path-or-snapshot> <mountpoint>`.
        #[arg(num_args = 1..=2)]
        paths: Vec<PathBuf>,
        /// Local state directory (metadata DB + chunk cache).
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Chunk cache budget (e.g. 10G, 512MiB). Suffixes are binary.
        #[arg(long, default_value = "10G", value_parser = parse_byte_size)]
        cache_size: u64,
        /// Allow other users to access the mount.
        #[arg(long)]
        allow_other: bool,
        /// Filesystem source name reported by mount tools.
        #[arg(long, default_value = "constellation")]
        fs_name: String,
        /// What fsync() waits for: "local" (journal on disk; background
        /// ship) or "s3" (record durable in the shared log).
        #[arg(long, default_value = "local")]
        fsync_mode: String,
        /// Chunk close policy: "through" waits for S3; "back" returns
        /// after the local durable queue is journaled.
        #[arg(long, default_value = "through")]
        write_mode: String,
        /// Enrol this state directory as a read-only cluster member.
        /// Only valid on its first mount; RO members do not count toward
        /// a continuation epoch's write-eligible roster.
        #[arg(long)]
        read_only_member: bool,
        /// Mount a snapshot selector through an automatically created clone.
        #[arg(long)]
        rw: bool,
        /// Destination path/name for `--rw`.
        #[arg(long, requires = "rw")]
        clone_name: Option<String>,
        /// Create a temporary clone and remove it on clean unmount.
        #[arg(long, requires = "rw", conflicts_with = "clone_name")]
        ephemeral: bool,
        /// Serve the embedded control UI on localhost. Zero disables it.
        /// Bare `--web-ui` listens on 8080; `--web-ui <port>` picks a port.
        #[arg(
            long,
            env = "CONSTELLATION_WEB_UI_PORT",
            default_value_t = 0,
            num_args = 0..=1,
            default_missing_value = "8080"
        )]
        web_ui: u16,
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
    /// Change a running mount's write policy. Switching to `through`
    /// drains the durable pending-upload queue before it takes effect.
    WriteMode {
        mode: String,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Inspect one file or directory through the running daemon.
    Inspect {
        path: String,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Inspect local cache state.
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
    },
    /// Read recent daemon logs.
    Log {
        #[command(subcommand)]
        command: LogCommand,
    },
    /// Create, list, and delete immutable subtree snapshots.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    /// Create an ordinary writable subtree from a snapshot.
    Clone {
        selector: String,
        destination: String,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Low-level inspection commands.
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
    /// Coordinated bucket garbage collection.
    Gc {
        #[command(subcommand)]
        command: GcCommand,
    },
    /// Check bucket, replica, and cache consistency.
    Fsck {
        #[arg(long)]
        s3: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        #[arg(long)]
        repair: bool,
        /// Explicitly release this expired partition lease while repairing.
        #[arg(long, requires = "repair")]
        force_release: Option<String>,
    },
}

#[derive(Subcommand)]
enum GcCommand {
    /// Mark, condemn, wait, and sweep eligible objects.
    Run {
        #[arg(long)]
        s3: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Include the expensive LIST-based orphan pass.
        #[arg(long)]
        orphans: bool,
    },
    /// Run the mark phase only and print deletion evidence.
    Verify {
        #[arg(long)]
        s3: String,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        #[arg(long)]
        orphans: bool,
    },
}

#[derive(Subcommand)]
enum SnapshotCommand {
    Create {
        selector: String,
        #[arg(long)]
        state_dir: PathBuf,
    },
    Ls {
        path: Option<String>,
        #[arg(long)]
        state_dir: PathBuf,
    },
    Delete {
        selector: String,
        #[arg(long)]
        state_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum CacheCommand {
    Ls {
        #[arg(long)]
        state_dir: PathBuf,
    },
    Stat {
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Drop clean LRU chunks from the local cache (pinned/dirty kept).
    #[command(visible_alias = "evict")]
    Prune {
        #[arg(long)]
        state_dir: PathBuf,
        /// Keep at most this many bytes used (e.g. `1G`, `512MiB`).
        /// Default `0` frees every clean chunk.
        #[arg(long, default_value = "0", value_parser = parse_byte_size)]
        target: u64,
    },
}

#[derive(Subcommand)]
enum LogCommand {
    Tail {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long, default_value_t = 100)]
        lines: usize,
    },
}

#[derive(Subcommand)]
enum DebugCommand {
    SnapRefs {
        id: String,
        #[arg(long)]
        state_dir: PathBuf,
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
        /// Protect content and filename-bearing metadata with a passphrase.
        #[arg(long)]
        e2e: bool,
    },
    /// Change an E2E filesystem's passphrase without re-encrypting data.
    Passwd {
        #[arg(long)]
        s3: String,
    },
}

fn passphrase(env: &str, prompt: &str) -> Result<Zeroizing<String>> {
    if let Ok(value) = std::env::var(env) {
        if value.is_empty() {
            bail!("{env} must not be empty");
        }
        return Ok(Zeroizing::new(value));
    }
    let value = rpassword::prompt_password(prompt)?;
    if value.is_empty() {
        bail!("passphrase must not be empty");
    }
    Ok(Zeroizing::new(value))
}

fn main() -> Result<()> {
    let log_buffer = log_buffer::LogBuffer::default();
    let log_writer = log_buffer.clone();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(move || log_writer.writer())
        .init();
    let cli = Cli::parse();
    let threads = parallelism::thread_plan();
    tracing::info!(
        cpus = threads.cpus,
        fuse_threads = threads.fuse,
        tokio_threads = threads.tokio,
        blocking_threads = threads.blocking,
        "selected host-aware thread plan"
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.tokio)
        .max_blocking_threads(threads.blocking)
        .enable_all()
        .build()?;

    match cli.command {
        Command::Fs {
            command:
                FsCommand::Create {
                    s3,
                    chunk_size,
                    compression,
                    e2e,
                },
        } => {
            constellation_fs_core::validate_chunk_size(chunk_size)?;
            let setting: CompressionSetting =
                compression.parse().map_err(|e| anyhow::anyhow!("{e}"))?;
            let backend = rt
                .block_on(backend::open_backend(&s3))
                .context("opening backend")?;
            let store = ChunkStore::new(backend.clone());
            let mut meta = FsMeta::new(chunk_size, &setting.to_string());
            meta.e2e = e2e;
            rt.block_on(store.create_fs(&meta))
                .context("creating filesystem")?;
            if e2e {
                let secret = passphrase("CONSTELLATION_PASSPHRASE", "New filesystem passphrase: ")?;
                rt.block_on(put_keyring(&backend, &secret))
                    .context("creating E2E keyring")?;
            }
            println!("created filesystem {} at {s3}", meta.uuid);
            println!("  chunk_size:  {chunk_size}");
            println!("  compression: {setting}");
            println!("  e2e:         {e2e}");
            Ok(())
        }
        Command::Fs {
            command: FsCommand::Passwd { s3 },
        } => {
            let backend = rt
                .block_on(backend::open_backend(&s3))
                .context("opening backend")?;
            let meta = rt.block_on(ChunkStore::new(backend.clone()).load_fs())?;
            if !meta.e2e {
                bail!("filesystem is not in E2E mode");
            }
            let old = passphrase("CONSTELLATION_PASSPHRASE", "Current passphrase: ")?;
            let new = passphrase(
                "CONSTELLATION_NEW_PASSPHRASE",
                "New filesystem passphrase: ",
            )?;
            rt.block_on(change_passphrase(&backend, &old, &new))
                .context("changing E2E passphrase")?;
            println!("passphrase changed; data-encryption keys were not rotated");
            Ok(())
        }
        Command::Doctor { s3 } => {
            let store = ChunkStore::new(
                rt.block_on(backend::open_backend(&s3))
                    .context("opening backend")?,
            );
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
                let store = ChunkStore::new(
                    rt.block_on(backend::open_backend(&s3))
                        .context("opening backend")?,
                );
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
        Command::WriteMode { mode, state_dir } => {
            let mode: writeback::WriteMode = mode.parse().map_err(anyhow::Error::msg)?;
            rt.block_on(control_call(
                &state_dir,
                constellation_api::Request::SetWriteMode {
                    mode: mode.as_str().into(),
                },
            ))
        }
        Command::Inspect { path, state_dir } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Inspect { path },
        )),
        Command::Cache { command } => match command {
            CacheCommand::Ls { state_dir } => rt.block_on(control_call(
                &state_dir,
                constellation_api::Request::CacheList,
            )),
            CacheCommand::Stat { state_dir } => {
                rt.block_on(control_call(&state_dir, constellation_api::Request::Status))
            }
            CacheCommand::Prune { state_dir, target } => rt.block_on(control_call(
                &state_dir,
                constellation_api::Request::CachePrune {
                    target_bytes: target,
                },
            )),
        },
        Command::Log {
            command: LogCommand::Tail { state_dir, lines },
        } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::LogTail { lines },
        )),
        Command::Snapshot { command } => match command {
            SnapshotCommand::Create {
                selector,
                state_dir,
            } => rt.block_on(control_call(
                &state_dir,
                constellation_api::Request::SnapshotCreate { selector },
            )),
            SnapshotCommand::Ls { path, state_dir } => rt.block_on(control_call(
                &state_dir,
                constellation_api::Request::SnapshotList { path },
            )),
            SnapshotCommand::Delete {
                selector,
                state_dir,
            } => rt.block_on(control_call(
                &state_dir,
                constellation_api::Request::SnapshotDelete { selector },
            )),
        },
        Command::Clone {
            selector,
            destination,
            state_dir,
        } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::Clone {
                selector,
                destination,
            },
        )),
        Command::Debug {
            command: DebugCommand::SnapRefs { id, state_dir },
        } => rt.block_on(control_call(
            &state_dir,
            constellation_api::Request::SnapRefs { id },
        )),
        Command::Gc { command } => {
            let (s3, state_dir, orphans, verify_only) = match command {
                GcCommand::Run {
                    s3,
                    state_dir,
                    orphans,
                } => (s3, state_dir, orphans, false),
                GcCommand::Verify {
                    s3,
                    state_dir,
                    orphans,
                } => (s3, state_dir, orphans, true),
            };
            let report = rt.block_on(run_gc_cli(&s3, state_dir, orphans, verify_only))?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::Fsck {
            s3,
            state_dir,
            repair,
            force_release,
        } => {
            let report = rt.block_on(run_fsck_cli(
                &s3,
                state_dir,
                repair,
                force_release.as_deref(),
            ))?;
            let code = report.exit_code();
            println!("{}", serde_json::to_string_pretty(&report)?);
            if code != 0 {
                std::process::exit(code);
            }
            Ok(())
        }
        Command::Mount {
            s3,
            paths,
            state_dir,
            cache_size,
            allow_other,
            fs_name,
            fsync_mode,
            write_mode,
            read_only_member,
            rw,
            clone_name,
            ephemeral,
            web_ui,
        } => {
            let (inner_path, mountpoint) = match paths.as_slice() {
                [mountpoint] => ("/".to_string(), mountpoint.clone()),
                [inner, mountpoint] => (inner.to_string_lossy().into_owned(), mountpoint.clone()),
                _ => unreachable!("clap enforces one or two mount paths"),
            };
            let fsync_s3 = match fsync_mode.as_str() {
                "local" => false,
                "s3" => true,
                other => bail!("invalid --fsync-mode {other:?} (expected local or s3)"),
            };
            let write_mode: writeback::WriteMode =
                write_mode.parse().map_err(anyhow::Error::msg)?;
            mount(
                rt,
                threads.fuse,
                &s3,
                &mountpoint,
                &inner_path,
                state_dir,
                cache_size,
                allow_other,
                fs_name,
                fsync_s3,
                write_mode,
                read_only_member,
                rw,
                clone_name,
                ephemeral,
                web_ui,
                log_buffer,
            )
        }
    }
}

async fn run_gc_cli(
    s3: &str,
    state_dir: Option<PathBuf>,
    orphans: bool,
    verify_only: bool,
) -> Result<gc::GcReport> {
    let backend = backend::open_backend(s3).await?;
    let plain = ChunkStore::new(backend.clone());
    let fsmeta = plain.load_fs().await?;
    let keys = if fsmeta.e2e {
        let secret = passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?;
        Some(load_keyring(&backend, &secret).await?)
    } else {
        None
    };
    let chunks = std::sync::Arc::new(match &keys {
        Some(keys) => ChunkStore::new_e2e(backend.clone(), keys.clone()),
        None => plain,
    });
    let logs = match keys {
        Some(keys) => constellation_store_s3::LogStore::new_e2e(backend.clone(), keys),
        None => constellation_store_s3::LogStore::new(backend.clone()),
    };
    let dir = state_dir.unwrap_or_else(|| default_state_dir(&fsmeta));
    std::fs::create_dir_all(&dir)?;
    let db = dir.join("meta.db");
    if !db.exists() {
        shipper::bootstrap(&db, &logs).await?;
    }
    let meta = std::sync::Arc::new(SqliteMeta::open(db)?);
    meta.backfill_deref_once()?;
    let caps = chunks.probe_conditional_writes().await?;
    let mode = if caps.etag_cas {
        constellation_store_s3::LeaseMode::Cas
    } else {
        constellation_store_s3::LeaseMode::SingleWriter
    };
    gc::run(backend, chunks, meta, mode, orphans, verify_only, None).await
}

async fn run_fsck_cli(
    s3: &str,
    state_dir: Option<PathBuf>,
    repair: bool,
    force_release: Option<&str>,
) -> Result<fsck::FsckReport> {
    let backend = backend::open_backend(s3).await?;
    let plain = ChunkStore::new(backend.clone());
    let fsmeta = plain.load_fs().await?;
    let keys = if fsmeta.e2e {
        let secret = passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?;
        Some(load_keyring(&backend, &secret).await?)
    } else {
        None
    };
    let chunks = std::sync::Arc::new(match &keys {
        Some(keys) => ChunkStore::new_e2e(backend.clone(), keys.clone()),
        None => plain,
    });
    let logs = match keys {
        Some(keys) => constellation_store_s3::LogStore::new_e2e(backend.clone(), keys),
        None => constellation_store_s3::LogStore::new(backend.clone()),
    };
    let dir = state_dir.unwrap_or_else(|| default_state_dir(&fsmeta));
    std::fs::create_dir_all(&dir)?;
    let db = dir.join("meta.db");
    if !db.exists() {
        shipper::bootstrap(&db, &logs).await?;
    }
    let meta = std::sync::Arc::new(SqliteMeta::open(db)?);
    meta.backfill_deref_once()?;
    let caps = chunks.probe_conditional_writes().await?;
    let mode = if caps.etag_cas {
        constellation_store_s3::LeaseMode::Cas
    } else {
        constellation_store_s3::LeaseMode::SingleWriter
    };
    let compression: CompressionSetting = fsmeta
        .compression
        .parse()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    fsck::run(
        backend,
        chunks,
        &logs,
        meta,
        Some(&dir),
        compression,
        mode,
        repair,
        force_release,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
fn mount(
    rt: tokio::runtime::Runtime,
    fuse_threads: usize,
    s3: &str,
    mountpoint: &std::path::Path,
    inner_path: &str,
    state_dir: Option<PathBuf>,
    cache_size: u64,
    allow_other: bool,
    fs_name: String,
    fsync_s3: bool,
    initial_write_mode: writeback::WriteMode,
    read_only_member: bool,
    rw_snapshot: bool,
    clone_name: Option<String>,
    ephemeral: bool,
    web_ui: u16,
    log_buffer: log_buffer::LogBuffer,
) -> Result<()> {
    let backend = rt
        .block_on(backend::open_backend(s3))
        .context("opening backend")?;
    let fsmeta = rt
        .block_on(ChunkStore::new(backend.clone()).load_fs())
        .context("loading filesystem (fs create first?)")?;
    let e2e_keys = if fsmeta.e2e {
        let secret = passphrase("CONSTELLATION_PASSPHRASE", "Filesystem passphrase: ")?;
        Some(
            rt.block_on(load_keyring(&backend, &secret))
                .context("unlocking E2E keyring (wrong passphrase?)")?,
        )
    } else {
        None
    };
    let store = std::sync::Arc::new(match &e2e_keys {
        Some(keys) => ChunkStore::new_e2e(backend.clone(), keys.clone()),
        None => ChunkStore::new(backend.clone()),
    });
    let state_dir = state_dir.unwrap_or_else(|| default_state_dir(&fsmeta));
    std::fs::create_dir_all(&state_dir)?;
    let log = match &e2e_keys {
        Some(keys) => constellation_store_s3::LogStore::new_e2e(backend, keys.clone()),
        None => constellation_store_s3::LogStore::new(backend),
    };
    let db_path = state_dir.join("meta.db");
    // Fresh node: rebuild the replica from checkpoint + log replay.
    if !db_path.exists() {
        rt.block_on(shipper::bootstrap(&db_path, &log))
            .context("bootstrapping metadata replica")?;
    }
    let meta = std::sync::Arc::new(SqliteMeta::open(&db_path)?);
    meta.backfill_deref_once()?;
    meta.scratch_purge_all()?;
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

    let cache = std::sync::Arc::new(match &e2e_keys {
        Some(keys) => {
            DiskCache::open_keyed(state_dir.join("cache"), cache_size, *keys.addressing_key())?
        }
        None => DiskCache::open(state_dir.join("cache"), cache_size)?,
    });
    let compression: CompressionSetting = fsmeta
        .compression
        .parse()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let snapshots = std::sync::Arc::new(snapshot::SnapshotManager::new(
        meta.clone(),
        store.clone(),
        compression,
        fsmeta.chunk_size,
        node_id,
    ));
    let selector = inner_path
        .contains('@')
        .then(|| snapshot::split_selector(inner_path))
        .transpose()?;
    if rw_snapshot && selector.is_none() {
        bail!("--rw is only valid when mounting <path>@<snapshot>");
    }
    let mut ephemeral_clone = None;
    let mounted_path = if let Some((source_path, snapshot_name)) = &selector {
        if rw_snapshot {
            let destination = if ephemeral {
                format!(
                    "/.constellation-ephemeral-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|duration| duration.as_millis())
                        .unwrap_or(0)
                )
            } else {
                let clone_name = clone_name
                    .as_deref()
                    .context("--rw snapshot mounts require --clone-name or --ephemeral")?;
                if clone_name.starts_with('/') {
                    snapshot::normalize_path(clone_name)
                } else {
                    let parent = source_path
                        .rsplit_once('/')
                        .map(|pair| pair.0)
                        .unwrap_or("");
                    snapshot::normalize_path(&format!("{parent}/{clone_name}"))
                }
            };
            rt.block_on(snapshots.clone_to(source_path, snapshot_name, &destination))?;
            if ephemeral {
                ephemeral_clone = Some(destination.clone());
            }
            destination
        } else {
            source_path.clone()
        }
    } else {
        snapshot::normalize_path(inner_path)
    };

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
    let write_mode = std::sync::Arc::new(writeback::WriteModeState::new(initial_write_mode));
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
    let _gc_task = {
        let interval = std::env::var("CONSTELLATION_GC_INTERVAL_S")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(gc::DEFAULT_GC_INTERVAL_S);
        let object_store = store.inner().clone();
        let chunks = store.clone();
        let meta = meta.clone();
        let gc_peers = peers.clone();
        rt.spawn(async move {
            let mut timer = tokio::time::interval(std::time::Duration::from_secs(interval.max(1)));
            timer.tick().await;
            loop {
                timer.tick().await;
                if let Err(error) = gc::run(
                    object_store.clone(),
                    chunks.clone(),
                    meta.clone(),
                    lease_mode,
                    false,
                    false,
                    Some(&gc_peers),
                )
                .await
                {
                    tracing::warn!(%error, "periodic bucket GC pass failed");
                }
            }
        })
    };
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
    let existence = crate::existence::Existence::from_env();
    let upload = std::sync::Arc::new(UploadRuntime::new(
        caps.create_if_absent,
        Some(coop.clone()),
        existence.clone(),
    ));
    let forward = forward::ForwardState::new();
    let placement = std::sync::Arc::new(placement::Placement::new());

    let departed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut fs = fusefs::ConstellationFs::new(
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
                write_mode: write_mode.clone(),
            }),
            coop: Some(coop.clone()),
            staging_dir: staging_dir.clone(),
            staging_budget: staging_budget.clone(),
            snapshots: snapshots.clone(),
        },
        fsmeta.chunk_size,
        compression,
    );
    if let Some((path, name)) = &selector {
        if rw_snapshot {
            fs.set_subtree_root(&mounted_path)?;
        } else {
            fs.set_snapshot_root(path, name)?;
        }
    } else {
        fs.set_subtree_root(&mounted_path)?;
    }

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
        forward: forward.clone(),
        placement: placement.clone(),
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
                    peers.probe_all().await;
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
    if peers.is_enabled() {
        let (placement, peers, keepers) = (placement.clone(), peers.clone(), keepers.clone());
        rt.spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let held: Vec<(String, u64)> = {
                    let keepers = keepers.lock().await;
                    keepers
                        .iter()
                        .filter_map(|(part, keeper)| {
                            keeper.ship_epoch().map(|epoch| (part.clone(), epoch))
                        })
                        .collect()
                };
                if held.is_empty() {
                    continue;
                }
                placement.gossip_rtts(&peers, node_id).await;
                for (part, epoch) in held {
                    if let Some(best) = placement.recommend(node_id, &peers) {
                        let _ = peers
                            .request_to_node(
                                best,
                                &constellation_net::Payload::LeaseOffer { part, epoch },
                            )
                            .await;
                    }
                }
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
            upload,
            forward,
            placement,
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
            upload.clone(),
            forward.clone(),
            placement.clone(),
        );
        rt.spawn(async move {
            let mut pending: Option<fusefs::SyncRequest> = None;
            'sync: loop {
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
                            } else if peers.request_lease(&part, None).await {
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
                            && peers.request_lease(&part, None).await
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
                        let result = match keepers.get_mut(&part) {
                            Some(k) if k.ship_epoch().is_some() && !k.is_lost() => {
                                let epoch = k.ship_epoch().unwrap();
                                if epochs.writes_ok() {
                                    k.release_local();
                                    Some(fusefs::HandoffResult {
                                        epoch,
                                        etag: None,
                                        head_seq: ship.last_shipped_seq(&part),
                                    })
                                } else if let Err(e) = upload_dirty_chunks(
                                    &cache,
                                    &meta,
                                    &chunk_store,
                                    compression,
                                    &upload,
                                    None,
                                    Some(&part),
                                )
                                .await
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
                                            Ok(etag) => Some(fusefs::HandoffResult {
                                                epoch,
                                                etag,
                                                head_seq: ship.last_shipped_seq(&part),
                                            }),
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
                        let _ = reply.send(result);
                    }
                    Some(fusefs::SyncRequest::Mutate {
                        part,
                        requester,
                        op,
                        reply,
                    }) => {
                        // Only inspect lease state under the keeper lock. The
                        // metadata transaction must not serialize unrelated
                        // lease maintenance or network requests.
                        let (ship_epoch, is_lost) = {
                            let keepers = keepers.lock().await;
                            keepers
                                .get(&part)
                                .map(|keeper| (keeper.ship_epoch(), keeper.is_lost()))
                                .unwrap_or((None, false))
                        };
                        let known_holder = if ship_epoch.is_some() {
                            node_id
                        } else if let Some(holder) = forward.cached_holder(&part) {
                            holder
                        } else {
                            constellation_store_s3::LeaseStore::new(
                                store_inner.clone(),
                                &part,
                                lease_mode,
                            )
                            .get()
                            .await
                            .ok()
                            .flatten()
                            .map(|(lease, _)| lease.holder)
                            .unwrap_or(0)
                        };
                        let outcome = forward::holder_execute(
                            &meta,
                            ship_epoch,
                            is_lost,
                            known_holder,
                            &op,
                        );
                        if matches!(
                            outcome,
                            constellation_meta::MutateOutcome::Accepted { .. }
                        ) {
                            if let Some(view) = lease_views.lock().unwrap().get(&part) {
                                view.touch();
                            }
                            placement.note_forwarded(requester);
                            pending = Some(fusefs::SyncRequest::Nudge);
                        }
                        let _ = reply.send(outcome);
                    }
                    Some(fusefs::SyncRequest::Forward { part, op, reply }) => {
                        let local_epoch = {
                            let keepers = keepers.lock().await;
                            keepers.get(&part).and_then(|keeper| keeper.ship_epoch())
                        };
                        let outcome = if let Some(epoch) = local_epoch {
                            match constellation_meta::execute_mutate(&meta, &op) {
                                Ok(records) => {
                                    if let Some(view) = lease_views.lock().unwrap().get(&part) {
                                        view.touch();
                                    }
                                    placement.note_local(node_id);
                                    constellation_meta::MutateOutcome::Accepted { epoch, records }
                                }
                                Err(constellation_meta::MetaError::Conflict) => {
                                    // We are the holder, so our own replica is
                                    // authoritative; the requester rebases from it.
                                    constellation_meta::MutateOutcome::Conflict {
                                        manifest: None,
                                    }
                                }
                                Err(error) => {
                                    constellation_meta::MutateOutcome::Errno(
                                        forward::meta_errno(&error),
                                    )
                                }
                            }
                        } else {
                            let mut holder = forward.cached_holder(&part);
                            if holder.is_none() {
                                let store = constellation_store_s3::LeaseStore::new(
                                    store_inner.clone(),
                                    &part,
                                    lease_mode,
                                );
                                holder = store
                                    .get()
                                    .await
                                    .ok()
                                    .flatten()
                                    .map(|(lease, _)| lease.holder)
                                    .filter(|holder| *holder != 0);
                                if let Some(holder) = holder {
                                    forward.note_holder(&part, holder);
                                }
                            }
                            if let Some(mut holder) = holder {
                                let mut outcome = forward::request_mutate(
                                    &peers,
                                    &forward,
                                    &part,
                                    node_id,
                                    holder,
                                    &op,
                                )
                                .await;
                                if let constellation_meta::MutateOutcome::NotHolder {
                                    holder: next,
                                } = outcome
                                {
                                    if next != 0 && next != holder {
                                        holder = next;
                                        outcome = forward::request_mutate(
                                            &peers,
                                            &forward,
                                            &part,
                                            node_id,
                                            holder,
                                            &op,
                                        )
                                        .await;
                                    }
                                }
                                if let constellation_meta::MutateOutcome::Accepted {
                                    epoch,
                                    ref records,
                                } = outcome
                                {
                                    if let Err(error) =
                                        forward::apply_accepted(&meta, &part, epoch, records)
                                    {
                                        tracing::warn!(
                                            %error,
                                            part,
                                            "failed to apply accepted forwarded mutation"
                                        );
                                        let _ = reply.send(Err(error.to_string()));
                                        continue;
                                    }
                                }
                                outcome
                            } else {
                                forward.clear_holder(&part);
                                constellation_meta::MutateOutcome::Busy
                            }
                        };
                        if matches!(
                            outcome,
                            constellation_meta::MutateOutcome::Accepted { .. }
                        ) {
                            pending = Some(fusefs::SyncRequest::Nudge);
                        }
                        let _ = reply.send(Ok(outcome));
                    }
                    Some(fusefs::SyncRequest::ApplyPushed {
                        part,
                        seq,
                        epoch,
                        holder_node,
                        payload,
                    }) => {
                        let holder_node = (holder_node != 0)
                            .then_some(holder_node)
                            .or_else(|| shipper::segment_node(&payload))
                            .unwrap_or(0);
                        let applied = ship
                            .lock()
                            .await
                            .try_apply_pushed(&part, seq, epoch, &payload)
                            .unwrap_or(false);
                        if applied {
                            forward.pushed_applied.fetch_add(
                                1,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            if holder_node != 0 {
                                forward.note_holder(&part, holder_node);
                            }
                        } else {
                            pending = Some(fusefs::SyncRequest::Nudge);
                        }
                    }
                    Some(fusefs::SyncRequest::ClaimOffer { part, epoch }) => {
                        tracing::debug!(part, epoch, "claiming offered lease");
                        let mut ship = ship.lock().await;
                        let mut keepers = keepers.lock().await;
                        if !keepers.contains_key(&part) {
                            let mut keeper = lease::LeaseKeeper::new(
                                constellation_store_s3::LeaseStore::new(
                                    store_inner.clone(),
                                    &part,
                                    lease_mode,
                                ),
                                node_id,
                            );
                            keeper.share_takeover_gate(epochs.blocks_takeover.clone());
                            lease_views
                                .lock()
                                .unwrap()
                                .insert(part.clone(), keeper.view());
                            keepers.insert(part.clone(), keeper);
                        }
                        let _ = peers.request_lease(&part, forward.cached_holder(&part)).await;
                        if let Some(keeper) = keepers.get_mut(&part) {
                            if shipper::acquire_lease_for(&mut ship, keeper, &part)
                                .await
                                .unwrap_or(false)
                            {
                                placement.mark_migrated();
                            }
                        }
                    }
                    Some(fusefs::SyncRequest::DrainInode { ino, reply }) => {
                        let result = upload_dirty_chunks(
                            &cache,
                            &meta,
                            &chunk_store,
                            compression,
                            &upload,
                            (ino != 0).then_some(ino),
                            None,
                        )
                        .await;
                        let _ = reply.send(result.map_err(|error| format!("{error:#}")));
                    }
                    Some(fusefs::SyncRequest::Barrier { ino, reply }) => {
                        // `--fsync-mode s3` is an inode/partition
                        // barrier, not a whole-mount backlog drain.
                        let mut r = upload_dirty_chunks(
                            &cache,
                            &meta,
                            &chunk_store,
                            compression,
                            &upload,
                            Some(ino),
                            None,
                        )
                        .await;
                        if r.is_ok() {
                            let part = meta.partition_of(ino).unwrap_or_else(|_| "p0".into());
                            let mut ship = ship.lock().await;
                            let mut keepers = keepers.lock().await;
                            r = match keepers.get_mut(&part) {
                                Some(keeper) => ship.sync_one(&part, keeper).await,
                                None => Err(anyhow::anyhow!(
                                    "no lease keeper for fsync partition {part}"
                                )),
                            };
                        }
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
                            upload_dirty_chunks(
                                &cache,
                                &meta,
                                &chunk_store,
                                compression,
                                &upload,
                                None,
                                None,
                            )
                            .await
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
                        // Keep polling one round while draining ordinary
                        // nudges. Dropping this future used to cancel the
                        // async upload side while already-started
                        // spawn_blocking encoders continued, so a close()
                        // storm could multiply CPU work and blocking threads.
                        let round = run_managed_sync_round(
                            &ship,
                            &keepers,
                            &epochs,
                            &meta,
                            &cache,
                            &chunk_store,
                            compression,
                            &upload,
                        );
                        tokio::pin!(round);
                        loop {
                            tokio::select! {
                                biased;
                                r = &mut round => {
                                    if let Err(e) = r {
                                        tracing::warn!(
                                            error = %e,
                                            "metadata sync failed; will retry"
                                        );
                                        spool.lock().unwrap().last_error = Some(format!("{e:#}"));
                                    } else {
                                        pins.refresh_all().await;
                                    }
                                    break;
                                }
                                msg = sync_rx.recv() => match msg {
                                    Some(fusefs::SyncRequest::Nudge) => {
                                        // Coalesced: the current round already
                                        // covers the work visible at its start.
                                    }
                                    Some(request) => {
                                        // Explicit operations retain their old
                                        // prompt-response behavior.
                                        pending = Some(request);
                                        break;
                                    }
                                    None => break 'sync,
                                },
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
        write_mode: write_mode.clone(),
        upload: upload.clone(),
        snapshots: snapshots.clone(),
        log_buffer,
        forward: forward.clone(),
        placement: placement.clone(),
    });
    {
        let _guard = rt.enter();
        if let Err(e) = constellation_api::serve(&state_dir, status.clone()) {
            tracing::warn!(error = %e, "control API unavailable");
        }
        if web_ui != 0 {
            match rt.block_on(constellation_api::web::serve(web_ui, status)) {
                Ok(address) => tracing::info!(%address, "web UI listening (localhost only)"),
                Err(error) => tracing::warn!(%error, "web UI unavailable"),
            }
        }
    }

    let options = vec![
        fuser::MountOption::FSName(fs_name),
        fuser::MountOption::DefaultPermissions,
    ];
    let acl = if allow_other {
        fuser::SessionACL::All
    } else {
        fuser::SessionACL::Owner
    };
    let mut options = options;
    if selector.is_some() && !rw_snapshot {
        options.push(fuser::MountOption::RO);
    }
    existence.spawn_seed(store.clone(), &rt);
    tracing::info!(?mountpoint, ?state_dir, fs = %fsmeta.uuid, "mounting");
    let mut fuse_config = fuser::Config::default();
    fuse_config.mount_options = options;
    fuse_config.acl = acl;
    fuse_config.n_threads = Some(fuse_threads);
    fuse_config.clone_fd = cfg!(target_os = "linux") && fuse_config.n_threads != Some(1);
    // Build an explicit Session so SIGINT/SIGTERM can unmount from inside
    // this process (via SessionUnmounter). Plain `fuser::mount` has no hook
    // for that; without it, Ctrl-C kills the process and leaves a dead
    // mountpoint that needs an external `fusermount3 -u`.
    let mut session = fuser::Session::new(fs, mountpoint, &fuse_config).context("FUSE mount")?;
    let mut unmounter = session.unmount_callable();
    rt.spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
                tracing::warn!("failed to install SIGINT handler; Ctrl-C will not unmount");
                return;
            };
            let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
                tracing::warn!("failed to install SIGTERM handler");
                return;
            };
            tokio::select! {
                _ = sigint.recv() => tracing::info!("SIGINT received; unmounting FUSE"),
                _ = sigterm.recv() => tracing::info!("SIGTERM received; unmounting FUSE"),
            }
            if let Err(e) = unmounter.unmount() {
                tracing::warn!(error = %e, "signal-triggered FUSE unmount failed");
            }
            // A second signal during the post-unmount drain aborts immediately
            // so a hung ship/upload cannot trap the process forever.
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
            tracing::error!("second signal during shutdown; exiting immediately");
            std::process::exit(130);
        }
        #[cfg(not(unix))]
        {
            let _ = unmounter;
            tracing::warn!("signal-driven FUSE unmount is only supported on Unix");
        }
    });
    session.run().context("FUSE session")?;

    // Clean unmount: ship the journal tail, checkpoint, then release the
    // lease so a peer does not have to wait out the TTL. Skip when we
    // already flushed and retired via `leave` — the registry record is
    // a tombstone and a second ship is unnecessary.
    tracing::info!("FUSE detached; draining uploads and shipping journal before exit");
    if let Some(path) = ephemeral_clone {
        remove_live_subtree(&meta, &path)
            .with_context(|| format!("removing ephemeral clone {path}"))?;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    if matches!(meta.kv_get("left")?.as_deref(), Some("1")) {
        tracing::info!("node already left; skipping final drain");
        return Ok(());
    }
    let pending = meta.pending_upload_count().unwrap_or(0);
    let backlog = constellation_meta::MetaStore::journal_len(&*meta).unwrap_or(0);
    tracing::info!(
        pending_uploads = pending,
        journal_backlog = backlog,
        "clean unmount drain starting"
    );
    let flush = rt.block_on(async {
        // Plan 05a step 2: an orderly unmount must not publish manifests
        // for chunks that never made it to S3. If a previous best-effort
        // eager upload (`try_upload_dirty`) failed and only logged, this
        // is the last chance to drain `pending_upload` before the
        // journal ships — an unmount that refuses to finish cleanly here
        // is strictly better than one that silently strands content.
        if let Err(e) =
            upload_dirty_chunks(&cache, &meta, &store, compression, &upload, None, None).await
        {
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
    tracing::info!("clean unmount drain complete");
    Ok(())
}

fn remove_live_subtree(meta: &SqliteMeta, path: &str) -> Result<()> {
    let ino = meta
        .resolve_path(path)?
        .with_context(|| format!("clone path {path} disappeared"))?;
    fn clear(meta: &SqliteMeta, ino: u64) -> Result<()> {
        for entry in constellation_meta::MetaStore::readdir(meta, ino)? {
            if entry.kind == constellation_fs_core::InodeKind::Dir {
                clear(meta, entry.ino)?;
                constellation_meta::MetaStore::rmdir(meta, ino, &entry.name)?;
            } else {
                constellation_meta::MetaStore::unlink(meta, ino, &entry.name)?;
            }
        }
        Ok(())
    }
    clear(meta, ino)?;
    let parent = meta
        .parent_of(ino)?
        .context("ephemeral clone cannot be the filesystem root")?;
    let name = path
        .rsplit('/')
        .find(|part| !part.is_empty())
        .context("ephemeral clone has no basename")?;
    constellation_meta::MetaStore::rmdir(meta, parent, name)?;
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
    forward: std::sync::Arc<forward::ForwardState>,
    placement: std::sync::Arc<placement::Placement>,
}

impl constellation_net::PeerService for P2pBridge {
    fn segment_published(&self, part: &str, seq: u64, epoch: u64, payload: Option<Vec<u8>>) {
        tracing::debug!(part, seq, epoch, "peer published a segment; syncing now");
        let request = match payload {
            Some(payload) => fusefs::SyncRequest::ApplyPushed {
                part: part.to_string(),
                seq,
                epoch,
                holder_node: 0,
                payload,
            },
            None => fusefs::SyncRequest::Nudge,
        };
        let _ = self.nudge.send(request);
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
                etag: None,
                head_seq: None,
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
                Ok(Some(handed)) => {
                    tracing::info!(
                        part,
                        requester,
                        epoch = handed.epoch,
                        "handed the lease to a peer"
                    );
                    constellation_net::Payload::LeaseHandoff {
                        part,
                        epoch: handed.epoch,
                        released: true,
                        etag: handed.etag,
                        head_seq: handed.head_seq,
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

    fn mutate_requested(
        &self,
        part: String,
        requester: u64,
        req_id: u64,
        _epoch_seen: u64,
        op: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>>
    {
        Box::pin(async move {
            self.forward.note_holder(&part, self.node_id);
            let (reply, receive) = tokio::sync::oneshot::channel();
            let outcome = if self
                .nudge
                .send(fusefs::SyncRequest::Mutate {
                    part,
                    requester,
                    op,
                    reply,
                })
                .is_ok()
            {
                receive
                    .await
                    .unwrap_or(constellation_meta::MutateOutcome::Busy)
            } else {
                constellation_meta::MutateOutcome::Busy
            };
            constellation_net::Payload::MutateReply {
                req_id,
                outcome: outcome.to_postcard().unwrap_or_default(),
            }
        })
    }

    fn lease_offered(&self, part: String, epoch: u64) {
        let _ = self
            .nudge
            .send(fusefs::SyncRequest::ClaimOffer { part, epoch });
    }

    fn peer_rtts(&self, node_id: u64, rtts: Vec<(u64, u16)>) {
        self.placement.note_peer_rtts(node_id, rtts);
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
    let relay = p2p.relay_label().to_string();
    let addr = p2p.addr();
    let pubkey = p2p.pubkey_hex();
    let peers = constellation_net::Peers::new(p2p, node_id);
    // Publish how peers reach us, then learn about them.
    match serde_json::to_value(&addr) {
        Ok(addr_json) => {
            if let Err(e) = constellation_store_s3::publish_p2p(
                store.clone(),
                node_id,
                &pubkey,
                addr_json,
                env!("CONSTELLATION_VERSION"),
            )
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
        %relay,
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
            let records: Vec<constellation_net::PeerEnrollment> = nodes
                .into_iter()
                .filter_map(|n| {
                    Some(constellation_net::PeerEnrollment {
                        node_id: n.node_id,
                        pubkey_hex: n.pubkey?,
                        addr_json: n.p2p_addr?,
                        hostname: n.hostname,
                        version: n.version.unwrap_or_default(),
                        created_unix: n.created_unix,
                        p2p_updated_unix: n.p2p_updated_unix,
                        ro: n.ro,
                    })
                })
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
        constellation_api::Response::Snapshots { snapshots } => {
            println!("{}", serde_json::to_string_pretty(&snapshots)?);
            Ok(())
        }
        constellation_api::Response::Refs { hashes } => {
            for hash in hashes {
                println!("{hash}");
            }
            Ok(())
        }
        constellation_api::Response::Status(status) => {
            println!("{}", serde_json::to_string_pretty(&status)?);
            Ok(())
        }
        constellation_api::Response::Inspection { entry } => {
            println!("{}", serde_json::to_string_pretty(&entry)?);
            Ok(())
        }
        constellation_api::Response::CacheEntries { entries } => {
            println!("{}", serde_json::to_string_pretty(&entries)?);
            Ok(())
        }
        constellation_api::Response::Logs { lines } => {
            for line in lines {
                println!("{line}");
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
use constellation_upload_concurrency::{AdaptiveConcurrency, ConcurrencyGate, ConcurrencyPermit};

/// Ceiling for the adaptive search and for a user-pinned override alike.
/// Bounds both real S3 concurrency and (via [`ConcurrencyGate`]) worst-case
/// pending-upload memory: at most this many chunk buffers are ever held
/// at once regardless of how many rows `pending_upload` has queued.
const UPLOAD_CONCURRENCY_HARD_MAX: usize = 128;

/// See docs/explanation/DESIGN.md §5b step 2 / `docs/history/v1/plans/05b-p5b-streaming-writeback.md`.
/// A durable pending-upload queue in SQLite is drained by a bounded pool;
/// the pool costs two things once it exists (dedup-probe RTT and the
/// create-vs-overwrite decision), both handled by `put_mode` below.
///
/// Concurrency itself is adaptive by default
/// (`constellation_upload_concurrency::AdaptiveConcurrency`): a single
/// upload's latency is dominated by RTT to the bucket region, so a
/// client far from the bucket but sitting on a fat pipe (e.g. a home
/// connection in the EU against a `us-west-2` bucket) needs a lot more
/// parallelism than one on a thin or nearby link to fill that
/// bandwidth-delay product, and a fixed pool size tuned for one path is
/// wrong for the other. `CONSTELLATION_UPLOAD_CONCURRENCY` still pins a
/// fixed value for anyone who wants to opt out of the search entirely.
///
/// The policy and gate live in their own crate
/// (`crates/upload-concurrency`) so `bench/uploadbench` can drive the
/// exact production algorithm against a synthetic or live S3 target,
/// rather than a reimplementation that could drift from what ships here.
struct UploadRuntime {
    gate: ConcurrencyGate,
    controller: Option<std::sync::Mutex<AdaptiveConcurrency>>,
    max_concurrency: usize,
    create_if_absent: bool,
    probe: std::sync::Mutex<writeback::ProbePolicy>,
    decisions: std::sync::atomic::AtomicU64,
    coop: Option<std::sync::Arc<crate::coop::Coop>>,
    existence: std::sync::Arc<crate::existence::Existence>,
}

impl UploadRuntime {
    fn new(
        create_if_absent: bool,
        coop: Option<std::sync::Arc<crate::coop::Coop>>,
        existence: std::sync::Arc<crate::existence::Existence>,
    ) -> Self {
        let max = std::env::var("CONSTELLATION_UPLOAD_MAX_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(UPLOAD_CONCURRENCY_HARD_MAX)
            .clamp(1, UPLOAD_CONCURRENCY_HARD_MAX);
        let fixed = std::env::var("CONSTELLATION_UPLOAD_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|n| n.clamp(1, max));
        let (initial, controller) = match fixed {
            Some(n) => (n, None),
            // Start conservatively (like TCP slow start) and let the
            // controller climb; an aggressive initial guess on a
            // constrained link just causes early retries/backoff.
            None => (
                4.min(max),
                Some(std::sync::Mutex::new(AdaptiveConcurrency::new(
                    4.min(max),
                    1,
                    max,
                ))),
            ),
        };
        debug_assert!(
            controller
                .as_ref()
                .is_none_or(|c| c.lock().unwrap().current() == initial),
            "gate and controller must start in agreement"
        );
        Self {
            gate: ConcurrencyGate::new(initial),
            controller,
            max_concurrency: max,
            create_if_absent,
            probe: std::sync::Mutex::new(writeback::ProbePolicy::default()),
            decisions: std::sync::atomic::AtomicU64::new(0),
            coop,
            existence,
        }
    }

    /// Hard ceiling on real concurrency and thus on worst-case pending-
    /// upload memory: at most this many chunk buffers are held at once,
    /// however many rows `pending_upload` has queued.
    fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    async fn permit(&self) -> ConcurrencyPermit<'_> {
        self.gate.acquire().await
    }

    fn record_success(&self, bytes: u64, latency: std::time::Duration, now: std::time::Instant) {
        let Some(controller) = &self.controller else {
            return;
        };
        let new_target = controller.lock().unwrap().on_success(now, bytes, latency);
        if new_target != self.gate.target() {
            tracing::debug!(
                concurrency = new_target,
                previous = self.gate.target(),
                "adaptive upload concurrency adjusted"
            );
            self.gate.set_target(new_target);
        }
    }

    fn record_error(&self, now: std::time::Instant) {
        let Some(controller) = &self.controller else {
            return;
        };
        let new_target = controller.lock().unwrap().on_error(now);
        let previous = self.gate.target();
        if new_target != previous {
            tracing::debug!(
                concurrency = new_target,
                previous,
                "upload failed; backing off adaptive concurrency"
            );
            self.gate.set_target(new_target);
        } else {
            tracing::debug!(
                concurrency = new_target,
                "upload failure coalesced with current congestion episode"
            );
        }
    }

    fn put_mode(
        &self,
        hash: &constellation_fs_core::ChunkHash,
    ) -> constellation_store_s3::ChunkPutMode {
        if self.existence.peer_hints_enabled()
            && self
                .coop
                .as_ref()
                .is_some_and(|coop| coop.peer_digest_contains(hash))
        {
            self.existence.note_peer_hint();
            return constellation_store_s3::ChunkPutMode::Probe;
        }
        match self.existence.contains(hash) {
            Some(true) => return constellation_store_s3::ChunkPutMode::Probe,
            Some(false) if self.create_if_absent => {
                return constellation_store_s3::ChunkPutMode::Create;
            }
            Some(false) => return constellation_store_s3::ChunkPutMode::Overwrite,
            None => {}
        }
        let n = self
            .decisions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.probe.lock().unwrap().enabled() || n.is_multiple_of(16) {
            constellation_store_s3::ChunkPutMode::Probe
        } else if self.create_if_absent {
            constellation_store_s3::ChunkPutMode::Create
        } else {
            constellation_store_s3::ChunkPutMode::Overwrite
        }
    }

    #[cfg(test)]
    fn for_test(create_if_absent: bool) -> Self {
        Self::new(
            create_if_absent,
            None,
            crate::existence::Existence::new(1024, false, false),
        )
    }

    /// Test helper: pin a fixed concurrency (no adaptive controller),
    /// bypassing environment variables so tests are hermetic.
    #[cfg(test)]
    fn for_test_fixed(create_if_absent: bool, concurrency: usize) -> Self {
        Self {
            gate: ConcurrencyGate::new(concurrency),
            controller: None,
            max_concurrency: concurrency.max(1),
            create_if_absent,
            probe: std::sync::Mutex::new(writeback::ProbePolicy::default()),
            decisions: std::sync::atomic::AtomicU64::new(0),
            coop: None,
            existence: crate::existence::Existence::new(1024, false, false),
        }
    }
}

async fn upload_dirty_chunks(
    cache: &DiskCache,
    meta: &SqliteMeta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
    only_ino: Option<constellation_fs_core::Ino>,
    only_part: Option<&str>,
) -> Result<()> {
    use futures::StreamExt;
    let mut grouped: std::collections::HashMap<
        constellation_fs_core::ChunkHash,
        Vec<constellation_fs_core::Ino>,
    > = std::collections::HashMap::new();
    for (hash, ino) in meta.pending_uploads()? {
        if only_ino.is_some_and(|wanted| ino != wanted) {
            continue;
        }
        if let Some(wanted) = only_part {
            if meta.partition_of(ino).ok().as_deref() != Some(wanted) {
                continue;
            }
        }
        grouped.entry(hash).or_default().push(ino);
    }
    let total = grouped.len() as u64;
    if total > 0 {
        tracing::debug!(
            pending_chunks = total,
            concurrency = upload.gate.target(),
            max_concurrency = upload.max_concurrency(),
            "uploading pending chunks"
        );
    }
    // Materialize at most the configured maximum number of upload
    // futures. Each one reads bytes only after winning the adaptive gate,
    // so both future state and chunk buffers stay independent of a backlog
    // that may contain tens of thousands of rows.
    let in_flight = futures::stream::iter(grouped.into_iter().map(|(hash, inos)| async move {
        let _permit = upload.permit().await;
        let Some(data) = cache.get(&hash)? else {
            tracing::error!(
                %hash,
                ?inos,
                "pending upload chunk missing from local cache (unrecoverable content); \
                 leaving the pending row and refusing to ship"
            );
            bail!("pending upload chunk {hash} missing from local cache");
        };
        let bytes = data.len() as u64;
        let mode = upload.put_mode(&hash);
        let mut last = None;
        let started = std::time::Instant::now();
        for attempt in 0..3 {
            match store.put_chunk_mode(&hash, &data, compression, mode).await {
                Ok(result) => {
                    let now = std::time::Instant::now();
                    // A Probe hit only performed HEAD; counting the
                    // chunk's logical bytes as uploaded would report
                    // impossible goodput and drive concurrency upward
                    // during deduplicated workloads.
                    if !result.existed {
                        upload.record_success(bytes, now.duration_since(started), now);
                    }
                    return Ok((hash, inos, mode, result.existed));
                }
                Err(error) => last = Some(error),
            }
            if attempt < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        upload.record_error(std::time::Instant::now());
        Err(anyhow::anyhow!("upload {hash} failed: {}", last.unwrap()))
    }))
    .buffer_unordered(upload.max_concurrency());
    futures::pin_mut!(in_flight);
    let mut first_error = None;
    let mut completed = 0u64;
    let mut last_progress = std::time::Instant::now();
    let progress_interval = std::env::var("CONSTELLATION_UPLOAD_PROGRESS_INTERVAL_S")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(10));
    while let Some(result) = in_flight.next().await {
        match result {
            Ok((hash, inos, mode, existed)) => {
                upload.existence.insert(&hash);
                if mode == constellation_store_s3::ChunkPutMode::Probe {
                    upload.probe.lock().unwrap().record(existed);
                }
                for ino in inos {
                    meta.ack_upload(&hash, ino)?;
                }
                if !meta.upload_pending_for_hash(&hash)? {
                    cache.set_state(&hash, constellation_fs_core::cache::ChunkState::Clean);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        completed += 1;
        if total > 0 && (completed == total || last_progress.elapsed() >= progress_interval) {
            tracing::info!(
                completed,
                total,
                concurrency = upload.gate.target(),
                "pending chunk upload progress"
            );
            last_progress = std::time::Instant::now();
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if total > 0 {
        tracing::debug!(uploaded = total, "pending chunk upload complete");
    }
    Ok(())
}

/// Drive either the ordinary S3 authority path or a continuation epoch.
/// An S3 failure may activate an epoch, but the failing round remains an
/// error for spool observability. While active, a successful tail probe
/// means S3 returned: upload dirty chunks first, close the promise, then
/// resume ordinary CAS-serialized shipping.
#[allow(clippy::too_many_arguments)]
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
    upload: &UploadRuntime,
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
        upload_dirty_chunks(cache, meta, store, compression, upload, None, None).await?;
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

    if let Err(error) =
        upload_dirty_chunks(cache, meta, store, compression, upload, None, None).await
    {
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

/// Format dial addresses for status/UI output.
fn peer_addr_strings(addr: &constellation_net::EndpointAddr) -> Vec<String> {
    addr.addrs.iter().map(|a| a.to_string()).collect()
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
    write_mode: std::sync::Arc<writeback::WriteModeState>,
    upload: std::sync::Arc<UploadRuntime>,
    snapshots: std::sync::Arc<snapshot::SnapshotManager>,
    log_buffer: log_buffer::LogBuffer,
    forward: std::sync::Arc<forward::ForwardState>,
    placement: std::sync::Arc<placement::Placement>,
}

impl DaemonStatus {
    /// Snapshot/clone control requests are metadata mutations too: acquire
    /// the subtree partition and force its pending data + journal through
    /// before observing or publishing an immutable root.
    fn snapshot_barrier(&self, path: &str) -> std::result::Result<(), String> {
        let ino = self
            .meta
            .resolve_path(path)
            .map_err(|error| error.to_string())?
            .unwrap_or(constellation_fs_core::types::ROOT_INO);
        let part = self
            .meta
            .partition_of(ino)
            .map_err(|error| error.to_string())?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Acquire { part, reply })
            .map_err(|_| "sync task is not running".to_string())?;
        let acquired = tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "lease acquisition stopped".to_string())??;
        if !acquired {
            return Err("subtree write lease is held by another node".into());
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::Barrier { ino, reply })
            .map_err(|_| "sync task is not running".to_string())?;
        tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "snapshot barrier stopped".to_string())?
    }
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
        let designations = self.list_designations();
        let epoch = self.epochs.status();
        let coop = self.coop.report();
        let s3_coop = coop.per_source.iter().find(|s| s.id == "s3").cloned();
        let peer_snap = self.peers.snapshot();
        let mut peers: Vec<constellation_api::PeerStatus> = Vec::with_capacity(1 + peer_snap.len());
        // S3 is always first so operators can compare the durable path
        // against peer lat/BW/hit% in the same table.
        peers.push(constellation_api::PeerStatus {
            node_id: 0,
            connected: true,
            hostname: Some("S3".into()),
            coop: s3_coop,
            s3: true,
            path: String::new(),
            ..Default::default()
        });
        peers.extend(peer_snap.into_iter().map(|p| {
            let addrs = peer_addr_strings(&p.addr);
            let designations: Vec<String> = designations
                .iter()
                .filter(|d| d.designee == p.node_id)
                .map(|d| d.path.clone())
                .collect();
            let coop = coop
                .per_source
                .iter()
                .find(|s| s.id == format!("peer-{}", p.node_id))
                .cloned();
            let path = match p.path {
                constellation_net::PathKind::Unknown => coop
                    .as_ref()
                    .map(|c| c.path.clone())
                    .filter(|s| !s.is_empty() && s != "unknown")
                    .unwrap_or_else(|| "unknown".into()),
                other => other.as_str().into(),
            };
            constellation_api::PeerStatus {
                node_id: p.node_id,
                connected: p.connected,
                rtt_ms: p.rtt_ms,
                last_seen_ms: p.last_seen.map(|t| t.elapsed().as_millis() as u64),
                hostname: (!p.hostname.is_empty()).then_some(p.hostname.clone()),
                version: (!p.version.is_empty()).then_some(p.version.clone()),
                pubkey: Some(p.pubkey_hex.clone()),
                endpoint_id: Some(p.addr.id.to_string()),
                addrs,
                created_unix: (p.created_unix > 0).then_some(p.created_unix),
                p2p_updated_unix: p.p2p_updated_unix,
                ro: p.ro,
                epoch_member: epoch.members.contains(&p.node_id),
                designations,
                coop,
                s3: false,
                path,
            }
        }));
        let p2p = constellation_api::P2pStatus {
            enabled: self.peers.is_enabled(),
            node_addr: self
                .peers
                .node_addr()
                .and_then(|a| serde_json::to_string(&a).ok()),
            relay: self.peers.relay_label(),
            peers,
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
            version: env!("CONSTELLATION_VERSION").to_string(),
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
                pinned_bytes: usage.pinned,
                staging_bytes: self.staging_budget.used(),
                staging_budget_bytes: self.staging_budget.budget(),
            },
            lease: p0_lease,
            partitions,
            p2p,
            pins: self.list_pins(),
            designations,
            epoch,
            reintegration: self
                .reintegration
                .snapshot(self.meta.unmarked_journal_len().unwrap_or(0)),
            coop,
            writeback: {
                let probe = self.upload.probe.lock().unwrap();
                let existence = self.upload.existence.report();
                constellation_api::WritebackStatus {
                    mode: self.write_mode.get().as_str().into(),
                    dirty_bytes: self
                        .cache
                        .dirty_bytes()
                        .saturating_add(self.staging_budget.used()),
                    pending_uploads: self.meta.pending_upload_count().unwrap_or(0),
                    upload_concurrency: self.upload.gate.target() as u32,
                    remote_probe_enabled: probe.enabled(),
                    remote_probe_hit_rate: probe.hit_rate(),
                    existence_listed: existence.listed,
                    existence_complete: existence.complete,
                    existence_bloom_hits: existence.bloom_hits,
                    existence_bloom_misses: existence.bloom_misses,
                    existence_peer_hints: existence.peer_hints,
                }
            },
            forwarded_ok: self.forward.ok.load(std::sync::atomic::Ordering::Relaxed),
            forwarded_err: self.forward.err.load(std::sync::atomic::Ordering::Relaxed),
            forward_p50_ms: self.forward.p50_ms(),
            pushed_segments_applied: self
                .forward
                .pushed_applied
                .load(std::sync::atomic::Ordering::Relaxed),
            placement_reason: self.placement.last_reason.lock().unwrap().clone(),
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

    fn set_write_mode(&self, mode: &str) -> std::result::Result<String, String> {
        let requested: writeback::WriteMode = mode.parse().map_err(str::to_string)?;
        if self.write_mode.get() == requested {
            return Ok(format!("write mode already {}", requested.as_str()));
        }
        if requested == writeback::WriteMode::Through {
            let (reply, receive) = tokio::sync::oneshot::channel();
            self.sync_tx
                .send(fusefs::SyncRequest::DrainInode { ino: 0, reply })
                .map_err(|_| "sync task is not running".to_string())?;
            tokio::task::block_in_place(|| {
                self.rt
                    .block_on(receive)
                    .map_err(|_| "upload drain stopped".to_string())?
            })?;
        }
        self.write_mode.set(requested);
        Ok(format!("write mode set to {}", requested.as_str()))
    }

    fn snapshot_create(&self, selector: &str) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        self.snapshot_barrier(&path)?;
        let snapshots = self.snapshots.clone();
        let result =
            tokio::task::block_in_place(|| self.rt.block_on(snapshots.create(&path, &name)))
                .map_err(|error| format!("{error:#}"))?;
        let _ = self.sync_tx.send(fusefs::SyncRequest::Nudge);
        Ok(result)
    }

    fn snapshot_list(
        &self,
        path: Option<&str>,
    ) -> std::result::Result<Vec<constellation_api::SnapshotStatus>, String> {
        self.snapshots
            .list(path)
            .map_err(|error| format!("{error:#}"))
            .map(|rows| {
                rows.into_iter()
                    .map(|row| constellation_api::SnapshotStatus {
                        id: row.id,
                        path: row.path,
                        name: row.name,
                        root_hash: row.root_hash,
                        created_unix_ms: row.created_unix_ms,
                    })
                    .collect()
            })
    }

    fn snapshot_delete(&self, selector: &str) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        self.snapshot_barrier(&path)?;
        let snapshots = self.snapshots.clone();
        let result =
            tokio::task::block_in_place(|| self.rt.block_on(snapshots.delete(&path, &name)))
                .map_err(|error| format!("{error:#}"))?;
        let _ = self.sync_tx.send(fusefs::SyncRequest::Nudge);
        Ok(result)
    }

    fn clone_snapshot(
        &self,
        selector: &str,
        destination: &str,
    ) -> std::result::Result<String, String> {
        let (path, name) = snapshot::split_selector(selector).map_err(|error| error.to_string())?;
        self.snapshot_barrier(&path)?;
        let snapshots = self.snapshots.clone();
        let destination = destination.to_string();
        let result = tokio::task::block_in_place(|| {
            self.rt
                .block_on(snapshots.clone_to(&path, &name, &destination))
        })
        .map_err(|error| format!("{error:#}"))?;
        let _ = self.sync_tx.send(fusefs::SyncRequest::Nudge);
        Ok(result)
    }

    fn snap_refs(&self, id: &str) -> std::result::Result<Vec<String>, String> {
        let snapshots = self.snapshots.clone();
        let id = id.to_string();
        tokio::task::block_in_place(|| self.rt.block_on(snapshots.refs(&id)))
            .map_err(|error| format!("{error:#}"))
    }

    fn read_dir(
        &self,
        path: &str,
    ) -> std::result::Result<Vec<constellation_api::DirectoryEntry>, String> {
        use constellation_meta::MetaStore;
        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let entries = self.meta.readdir(ino).map_err(|error| error.to_string())?;
        Ok(entries
            .into_iter()
            .map(|entry| constellation_api::DirectoryEntry {
                path: if normalized == "/" {
                    format!("/{}", entry.name)
                } else {
                    format!("{normalized}/{}", entry.name)
                },
                name: entry.name,
                ino: entry.ino,
                kind: format!("{:?}", entry.kind).to_lowercase(),
            })
            .collect())
    }

    fn inspect(&self, path: &str) -> std::result::Result<constellation_api::InspectStatus, String> {
        use constellation_meta::MetaStore;
        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let attr = self
            .meta
            .getattr(ino)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: stale inode"))?;
        let manifest = self
            .meta
            .manifest(ino)
            .map_err(|error| error.to_string())?
            .map(|bytes| {
                constellation_fs_core::manifest::Manifest::decode(&bytes)
                    .map(|manifest| constellation_api::ManifestStatus {
                        chunk_size: manifest.layout.chunk_size,
                        chunk_count: manifest.layout.chunk_count(manifest.file_len),
                        spilled: manifest.is_spilled(),
                    })
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        Ok(constellation_api::InspectStatus {
            path: normalized,
            ino: attr.ino,
            kind: format!("{:?}", attr.kind).to_lowercase(),
            size: attr.size,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            nlink: attr.nlink,
            atime_ns: attr.atime_ns,
            mtime_ns: attr.mtime_ns,
            ctime_ns: attr.ctime_ns,
            rdev: attr.rdev,
            manifest,
        })
    }

    fn open_download(
        &self,
        path: &str,
    ) -> std::result::Result<constellation_api::DownloadSession, String> {
        use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
        use constellation_fs_core::InodeKind;
        use constellation_meta::MetaStore;

        let normalized = normalize_control_path(path);
        let ino = self
            .meta
            .resolve_path(&normalized)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: not found"))?;
        let attr = self
            .meta
            .getattr(ino)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{normalized}: stale inode"))?;
        if attr.kind != InodeKind::File {
            return Err(format!("{normalized}: not a regular file"));
        }
        let file_name = normalized
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or("download")
            .to_string();
        let size = attr.size;
        let manifest_bytes = self.meta.manifest(ino).map_err(|error| error.to_string())?;
        let manifest = match manifest_bytes {
            Some(bytes) => Manifest::decode(&bytes).map_err(|error| error.to_string())?,
            None => Manifest::empty(constellation_fs_core::DEFAULT_CHUNK_SIZE),
        };
        if manifest.file_len != size && size > 0 {
            // Prefer the inode size as the wire length; still stream from the
            // manifest's chunk map so a stale size cannot OOM the client.
            tracing::debug!(
                path = %normalized,
                inode_size = size,
                manifest_len = manifest.file_len,
                "download size mismatch; using inode size"
            );
        }

        let coop = self.coop.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, String>>(2);
        self.rt.spawn(async move {
            if size == 0 {
                return;
            }
            let hashes = match &manifest.chunks {
                ChunkInfo::Inline(map) => map.clone(),
                ChunkInfo::Spilled(hash) => match coop.fetch(hash).await {
                    Ok(blob) => match decode_chunk_list(&blob) {
                        Ok(map) => map,
                        Err(error) => {
                            let _ = tx.send(Err(error.to_string())).await;
                            return;
                        }
                    },
                    Err(error) => {
                        let _ = tx
                            .send(Err(format!("fetching spilled chunk list: {error:#}")))
                            .await;
                        return;
                    }
                },
            };
            let layout = manifest.layout;
            let file_len = size;
            let count = layout.chunk_count(file_len);
            for index in 0..count {
                let want = layout.chunk_len(file_len, index) as usize;
                let mut data = match hashes.get(&index) {
                    Some(hash) => match coop.fetch(hash).await {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            let _ = tx
                                .send(Err(format!("fetching chunk {index}: {error:#}")))
                                .await;
                            return;
                        }
                    },
                    None => Vec::new(),
                };
                data.resize(want, 0);
                if tx.send(Ok(data)).await.is_err() {
                    return;
                }
            }
        });
        Ok(constellation_api::DownloadSession {
            file_name,
            size,
            chunks: rx,
        })
    }

    fn force_release(&self, part: &str) -> std::result::Result<String, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.sync_tx
            .send(fusefs::SyncRequest::HandOff {
                part: part.to_string(),
                reply,
            })
            .map_err(|_| "sync task is not running".to_string())?;
        let handed = tokio::task::block_in_place(|| self.rt.block_on(receive))
            .map_err(|_| "lease release task stopped".to_string())?;
        match handed {
            Some(handed) => Ok(format!(
                "voluntarily released {part} at epoch {}; this was cooperative, not fencing",
                handed.epoch
            )),
            None => Err(format!(
                "{part} was not held locally or could not be flushed; no fencing was attempted"
            )),
        }
    }

    fn log_tail(&self, lines: usize) -> Vec<String> {
        self.log_buffer.tail(lines)
    }

    fn doctor(&self) -> std::result::Result<constellation_api::DoctorStatus, String> {
        let store = ChunkStore::new(self.store.clone());
        tokio::task::block_in_place(|| self.rt.block_on(store.probe_conditional_writes()))
            .map(|caps| constellation_api::DoctorStatus {
                create_if_absent: caps.create_if_absent,
                etag_cas: caps.etag_cas,
            })
            .map_err(|error| error.to_string())
    }

    fn cache_list(&self) -> Vec<constellation_api::CacheEntryStatus> {
        self.cache
            .entries()
            .into_iter()
            .map(|(hash, size, state)| constellation_api::CacheEntryStatus {
                hash: hash.to_hex(),
                size,
                state: format!("{state:?}").to_lowercase(),
            })
            .collect()
    }

    fn cache_prune(&self, target_bytes: u64) -> std::result::Result<String, String> {
        let report = self
            .cache
            .prune_to(target_bytes)
            .map_err(|e| e.to_string())?;
        Ok(format!(
            "pruned {} chunks ({} bytes); {} bytes remain \
             ({} pinned, {} dirty, {} entries)",
            report.freed_chunks,
            report.freed_bytes,
            report.used_bytes,
            report.pinned_bytes,
            report.dirty_bytes,
            report.entries
        ))
    }
}

fn normalize_control_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        "/".into()
    } else {
        format!("/{}", parts.join("/"))
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

/// Parse a human-readable byte size for CLI flags (`10G`, `512MiB`, bare
/// integer). Suffixes K/M/G/T (and KiB/MiB/…, KB/MB/…) are binary
/// (1024-based): `10G` means 10 GiB, matching the common cache-budget
/// convention rather than SI decimal.
fn parse_byte_size(input: &str) -> Result<u64, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty byte size".into());
    }
    let digits = s
        .char_indices()
        .take_while(|(_, c)| c.is_ascii_digit())
        .last()
        .map(|(i, _)| i + 1)
        .unwrap_or(0);
    if digits == 0 {
        return Err(format!("invalid byte size: {input}"));
    }
    let num: u64 = s[..digits]
        .parse()
        .map_err(|e| format!("invalid byte size: {e}"))?;
    let unit = s[digits..].trim().to_ascii_lowercase();
    let mult: u64 = match unit.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "ki" | "kib" => 1 << 10,
        "m" | "mb" | "mi" | "mib" => 1 << 20,
        "g" | "gb" | "gi" | "gib" => 1 << 30,
        "t" | "tb" | "ti" | "tib" => 1 << 40,
        _ => return Err(format!("unknown size unit in {input:?}")),
    };
    num.checked_mul(mult)
        .ok_or_else(|| format!("byte size overflow: {input}"))
}

#[cfg(test)]
mod parse_byte_size_tests {
    use super::*;

    #[test]
    fn binary_suffixes() {
        assert_eq!(parse_byte_size("10G").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size("10GiB").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size("10g").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size("64M").unwrap(), 64 << 20);
        assert_eq!(parse_byte_size("64MiB").unwrap(), 64 << 20);
        assert_eq!(parse_byte_size("512KiB").unwrap(), 512 << 10);
        assert_eq!(parse_byte_size("1T").unwrap(), 1 << 40);
    }

    #[test]
    fn bare_bytes_and_whitespace() {
        assert_eq!(parse_byte_size("10737418240").unwrap(), 10 << 30);
        assert_eq!(parse_byte_size(" 64M ").unwrap(), 64 << 20);
        assert_eq!(parse_byte_size("0").unwrap(), 0);
    }

    #[test]
    fn rejects_junk() {
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("G").is_err());
        assert!(parse_byte_size("10X").is_err());
        assert!(parse_byte_size("10.5G").is_err());
    }

    #[test]
    fn mount_default_is_ten_gib() {
        let cli = Cli::try_parse_from(["constellation", "mount", "--s3", "file:///tmp/x", "/mnt"])
            .unwrap();
        match cli.command {
            Command::Mount { cache_size, .. } => assert_eq!(cache_size, 10 << 30),
            _ => panic!("expected Mount"),
        }
    }

    #[test]
    fn mount_accepts_human_cache_size() {
        let cli = Cli::try_parse_from([
            "constellation",
            "mount",
            "--s3",
            "file:///tmp/x",
            "/mnt",
            "--cache-size",
            "64M",
        ])
        .unwrap();
        match cli.command {
            Command::Mount { cache_size, .. } => assert_eq!(cache_size, 64 << 20),
            _ => panic!("expected Mount"),
        }
    }

    #[test]
    fn bare_web_ui_defaults_to_8080() {
        let cli = Cli::try_parse_from([
            "constellation",
            "mount",
            "--s3",
            "file:///tmp/x",
            "/mnt",
            "--web-ui",
        ])
        .unwrap();
        match cli.command {
            Command::Mount { web_ui, .. } => assert_eq!(web_ui, 8080),
            _ => panic!("expected Mount"),
        }
    }

    #[test]
    fn web_ui_port_override() {
        let cli = Cli::try_parse_from([
            "constellation",
            "mount",
            "--s3",
            "file:///tmp/x",
            "/mnt",
            "--web-ui",
            "9090",
        ])
        .unwrap();
        match cli.command {
            Command::Mount { web_ui, .. } => assert_eq!(web_ui, 9090),
            _ => panic!("expected Mount"),
        }
    }
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
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Wraps an in-memory backend and can be told to fail every `put`,
    /// simulating a cut S3 path without needing toxiproxy for a unit
    /// test.
    #[derive(Debug)]
    struct FailingStore {
        inner: InMemory,
        fail_puts: AtomicBool,
        delay_puts: AtomicBool,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        puts: AtomicUsize,
        heads: AtomicUsize,
    }

    impl FailingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                fail_puts: AtomicBool::new(false),
                delay_puts: AtomicBool::new(false),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                puts: AtomicUsize::new(0),
                heads: AtomicUsize::new(0),
            })
        }

        fn set_fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }

        fn set_delay_puts(&self, delay: bool) {
            self.delay_puts.store(delay, Ordering::SeqCst);
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
            self.puts.fetch_add(1, Ordering::SeqCst);
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(active, Ordering::SeqCst);
            if self.delay_puts.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            if self.fail_puts.load(Ordering::SeqCst) {
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                return Err(object_store::Error::Generic {
                    store: "FailingStore",
                    source: "S3 path is cut (test injection)".into(),
                });
            }
            let result = self.inner.put_opts(location, payload, opts).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
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
            if options.head {
                self.heads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
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

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct Fixture {
        meta: SqliteMeta,
        cache: Arc<DiskCache>,
        cache_dir: PathBuf,
        store: Arc<ChunkStore>,
        failing: Arc<FailingStore>,
        _cache_tmp: tempfile::TempDir,
    }

    impl Fixture {
        /// Reopen the cache from the same directory: `DiskCache::open`
        /// rebuilds accounting purely from what is on disk, the same
        /// path a real remount after `kill -9` takes.
        fn reopen_cache_simulating_crash(&mut self) {
            self.cache = Arc::new(DiskCache::open(&self.cache_dir, 64 * 1024 * 1024).unwrap());
        }
    }

    fn fixture() -> Fixture {
        let failing = FailingStore::new();
        let cache_tmp = tempfile::tempdir().unwrap();
        let cache_dir = cache_tmp.path().to_path_buf();
        Fixture {
            meta: SqliteMeta::open_in_memory().unwrap(),
            cache: Arc::new(DiskCache::open(&cache_dir, 64 * 1024 * 1024).unwrap()),
            cache_dir,
            store: Arc::new(ChunkStore::new(failing.clone() as Arc<dyn ObjectStore>)),
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
            &UploadRuntime::for_test(true),
            None,
            None,
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
            &UploadRuntime::for_test(true),
            None,
            None,
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
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn upload_pool_honours_its_bound() {
        let f = fixture();
        f.failing.set_delay_puts(true);
        for i in 0..12u8 {
            let file = f
                .meta
                .create(
                    constellation_fs_core::types::ROOT_INO,
                    &format!("f-{i}"),
                    0o644,
                    0,
                    0,
                )
                .unwrap();
            let data = vec![i; 4096];
            let hash = ChunkHash::of(&data);
            f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
            f.meta
                .set_manifest_dirty(file.ino, b"M", data.len() as u64, &[hash])
                .unwrap();
        }
        let upload = UploadRuntime::for_test_fixed(true, 3);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert!(f.failing.max_in_flight.load(Ordering::SeqCst) <= 3);
        assert!(
            f.failing.max_in_flight.load(Ordering::SeqCst) >= 2,
            "the test must observe actual parallelism"
        );
    }

    fn queue(f: &Fixture, name: &str, data: &[u8]) -> ChunkHash {
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, name, 0o644, 0, 0)
            .unwrap();
        let hash = ChunkHash::of(data);
        f.cache.insert(&hash, data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, b"M", data.len() as u64, &[hash])
            .unwrap();
        hash
    }

    #[test]
    fn complete_list_seed_probes_hits_and_creates_misses_without_head() {
        let f = fixture();
        let known_data = b"already in S3";
        let known = ChunkHash::of(known_data);
        rt().block_on(f.store.put_chunk_mode(
            &known,
            known_data,
            CompressionSetting::RAW,
            constellation_store_s3::ChunkPutMode::Create,
        ))
        .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);

        queue(&f, "known", known_data);
        queue(&f, "new", b"not in S3");
        let existence = crate::existence::Existence::new(1024, true, false);
        existence.seed_for_test(&[known], true);
        let upload = UploadRuntime::new(true, None, existence);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();

        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn bloom_false_positive_still_calls_store_before_ack() {
        let f = fixture();
        let data = b"forced false positive";
        let hash = queue(&f, "false-positive", data);
        let existence = crate::existence::Existence::new(1024, true, false);
        existence.seed_for_test(&[hash], true);
        let upload = UploadRuntime::new(true, None, existence);

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn peer_hit_selects_probe_but_peer_miss_retains_adaptive_head() {
        let f = fixture();
        let hinted = ChunkHash::of(b"hinted");
        let bloom = constellation_net::Bloom::from_hashes(&[hinted.0]);
        let coop = crate::coop::Coop::new_for_upload_test(f.cache.clone(), f.store.clone());
        coop.apply_digest(constellation_net::DigestSnapshot {
            node_id: 2,
            generation: 1,
            bits: bloom.bits,
            nbits: bloom.nbits,
            k: bloom.k,
            n: bloom.n,
            bucket: 0,
            buckets: 1,
        });
        let existence = crate::existence::Existence::new(1024, true, true);
        let upload = UploadRuntime::new(true, Some(coop), existence);
        assert_eq!(
            upload.put_mode(&hinted),
            constellation_store_s3::ChunkPutMode::Probe
        );
        assert_eq!(upload.existence.report().peer_hints, 1);
        assert_eq!(
            upload.put_mode(&ChunkHash::of(b"peer miss")),
            constellation_store_s3::ChunkPutMode::Probe,
            "an incomplete LIST plus peer miss must keep the adaptive probe"
        );
    }

    #[test]
    fn condemned_hash_overwrites_even_when_existence_bloom_claims_present() {
        let f = fixture();
        let data = b"condemned existence hit";
        let hash = queue(&f, "condemned", data);
        rt().block_on(constellation_store_s3::publish_condemned(
            f.store.inner(),
            vec![hash.to_hex()],
            1,
        ))
        .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);
        let existence = crate::existence::Existence::new(1024, true, false);
        existence.seed_for_test(&[hash], true);
        let upload = UploadRuntime::new(true, None, existence);

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(
            f.failing.heads.load(Ordering::SeqCst),
            0,
            "condemned must bypass the hinted HEAD and overwrite"
        );
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }
}
